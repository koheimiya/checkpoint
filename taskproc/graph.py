""" DAG processor """
from __future__ import annotations
from exceptiongroup import ExceptionGroup
from contextlib import ExitStack
from datetime import datetime
from typing import Any, Mapping
from typing_extensions import Self, runtime_checkable, Protocol
from collections import defaultdict, Counter
from dataclasses import dataclass
from concurrent.futures import Future, ProcessPoolExecutor, wait, FIRST_COMPLETED, Executor
from pathlib import Path
import logging

from tqdm.auto import tqdm
import cloudpickle
import networkx as nx

from taskproc.executors import LocalExecutor

from .types import ErrorHandlingPolicy, JsonStr, TaskKey


LOGGER = logging.getLogger(__name__)


TaskLabels = tuple[str, ...]


@runtime_checkable
class TaskWorkerProtocol(Protocol):
    @property
    def labels(self) -> TaskLabels: ...
    @property
    def source_timestamp(self) -> datetime: ...
    @property
    def directory(self) -> Path: ...
    def to_tuple(self) -> TaskKey: ...
    def get_prerequisites(self) -> Mapping[str, TaskWorkerProtocol]: ...
    def peek_timestamp(self) -> datetime | None: ...
    def set_result(self, on_child_process: bool, interactive: bool, prefix_command: str | None) -> None: ...
    def dump_error_msg(self) -> str: ...


@dataclass
class TaskGraph:
    G: nx.DiGraph
    detect_source_change: bool

    @classmethod
    def build_from(cls, root: TaskWorkerProtocol, detect_source_change: bool) -> Self:
        G = nx.DiGraph()
        seen: set[TaskKey] = set()
        to_expand = [root]
        while to_expand:
            task = to_expand.pop()
            x = task.to_tuple()
            if x not in seen:
                seen.add(x)
                prerequisite_tasks = list(task.get_prerequisites().values())
                to_expand.extend(prerequisite_tasks)
                G.add_node(x, task=task, timestamp=task.peek_timestamp(), source_timestamp=task.source_timestamp)
                G.add_edges_from([(p.to_tuple(), x) for p in prerequisite_tasks])
        out = TaskGraph(G, detect_source_change=detect_source_change)
        out.trim()
        return out

    @property
    def size(self) -> int:
        return len(self.G)

    def get_task(self, key: TaskKey) -> TaskWorkerProtocol:
        return self.G.nodes[key]['task']

    def trim(self) -> None:
        self._mark_nodes_to_update()
        self._remove_fresh_nodes()
        self._transitive_reduction()

    def _mark_nodes_to_update(self) -> None:
        for x in nx.topological_sort(self.G):
            ts_task = self.G.nodes[x]['timestamp']
            ts_source = self.G.nodes[x]['source_timestamp']
            if ts_task is None or (self.detect_source_change and ts_task < ts_source):
                self.G.add_node(x, to_update=True)
                continue
            for p in self.G.predecessors(x):
                pred_to_update = self.G.nodes[p]['to_update']
                ts_pred = self.G.nodes[p]['timestamp']
                if pred_to_update or ts_task < ts_pred:
                    self.G.add_node(x, to_update=True)
                    break
            else:
                self.G.add_node(x, to_update=False)

    def _remove_fresh_nodes(self) -> None:
        to_remove = [x for x, attr in self.G.nodes.items() if not attr['to_update']]
        self.G.remove_nodes_from(to_remove)

    def _transitive_reduction(self) -> None:
        TR = nx.transitive_reduction(self.G)
        TR.add_nodes_from(self.G.nodes(data=True))
        self.G = TR

    def get_initial_tasks(self) -> dict[TaskLabels, list[TaskKey]]:
        leaves = [x for x in self.G if self.G.in_degree(x) == 0]
        return self._group_by_channels(leaves)

    def _group_by_channels(self, nodes: list[TaskKey]) -> dict[TaskLabels, list[TaskKey]]:
        out = defaultdict(list)
        for x in nodes:
            out[self.get_task(x).labels].append(x)
        return dict(out)

    def pop_with_new_leaves(self, x: TaskKey, disallow_non_leaf: bool = True) -> dict[TaskLabels, list[TaskKey]]:
        if disallow_non_leaf:
            assert not list(self.G.predecessors(x))

        new_leaves: list[TaskKey] = []
        for y in self.G.successors(x):
            if self.G.in_degree(y) == 1:
                new_leaves.append(y)

        self.G.remove_node(x)
        return self._group_by_channels(new_leaves)

    def get_nodes_by_task(self) -> dict[str, list[JsonStr]]:
        out: dict[str, list[JsonStr]] = defaultdict(list)
        for x in self.G:
            path, args = x
            out[path].append(args)
        return dict(out)


def run_task_graph(
        graph: TaskGraph,
        executor: Executor,
        rate_limits: dict[str, int],
        prefixes: dict[str, str],
        error_handling: ErrorHandlingPolicy,
        verbose_stats: bool,
        show_progress: bool,
        ) -> dict[str, Any]:
    """ Consume task graph concurrently.
    """
    is_local = isinstance(executor, LocalExecutor)
    is_process_pool = isinstance(executor, ProcessPoolExecutor)

    if show_progress and is_local:
        show_progress = False
        LOGGER.warning(f'LocalExecutor is detected while `show_progress` is set True. The progress bars is turned off.')

    stats = {k: len(args) for k, args in graph.get_nodes_by_task().items()}
    LOGGER.debug(f'Following tasks will be called: {stats}')
    info = {'stats': stats, 'in_process': [], 'remaining': []}

    if show_progress:
        progressbars = {
                k: tqdm(range(n), desc=k, position=i, mininterval=.1, maxinterval=1)
                for i, (k, n) in enumerate(stats.items())
                }
    else:
        progressbars = {}

    # Channel-wise concurrency tracker
    occupation: dict[str, set[Future[Any]]] = {k: set() for k in rate_limits}

    # Execute tasks
    standby = graph.get_initial_tasks()
    in_process: dict[Future[tuple[TaskLabels, TaskKey]], TaskKey] = dict()
    exceptions: list[FailedTaskError] = []

    with ExitStack() as stack:
        for pbar in progressbars.values():
            stack.enter_context(pbar)
        executor = stack.enter_context(executor)

        while standby or in_process:
            # Short circuit for eager error handling
            if error_handling == 'eager' and exceptions:
                break

            # Log some stats
            LOGGER.debug(
                    f'nodes: {graph.size}, '
                    f'standby: {len(standby)}, '
                    f'in_process: {len(in_process)}'
                    )
            if verbose_stats:
                info['remaining'].append(graph.get_nodes_by_task())

            # Submit all leaf tasks
            leftover: dict[TaskLabels, list[TaskKey]] = {}
            for channels, keys in standby.items():
                # Select tasks to submit up to rate_limits
                if any(chan in rate_limits for chan in channels):
                    free = min(rate_limits[chan] - len(occupation[chan]) for chan in channels if chan in rate_limits)
                    to_submit, to_hold = keys[:free], keys[free:]
                    if to_hold:
                        leftover[channels] = to_hold
                else:
                    to_submit = keys

                # Create futures
                futures_to_submit: list[Future[Any]] = []
                for key in to_submit:
                    if is_local:
                        LOGGER.info(f'Interactively executing {key}')

                    runner = _TaskRunner(
                            channels=channels,
                            task_data=cloudpickle.dumps(graph.get_task(key)),
                            on_child_process=is_process_pool,
                            interactive=is_local,
                            prefix_command=_get_prefix_command(channels=channels, prefixes=prefixes),
                            )
                    future = executor.submit(runner)
                    in_process[future] = key
                    futures_to_submit.append(future)

                for chan in channels:
                    if chan in occupation:
                        occupation[chan].update(futures_to_submit)

            if verbose_stats:
                def _summarize_tasks_in_process(taskkeys: list[TaskKey]):
                    out: dict[str, int] = defaultdict(lambda: 0)
                    for task_name, _ in taskkeys:
                        out[task_name] += 1
                    return dict(out)
                info['in_process'].append(_summarize_tasks_in_process(list(in_process.values())))

            # Wait for any tasks to complete
            done, _ = wait(in_process.keys(), return_when=FIRST_COMPLETED)

            # Update graph
            standby = defaultdict(list, leftover)
            for done_future in done:
                # Update occupied
                for chan, occ in occupation.items():
                    if done_future in occ:
                        occ.remove(done_future)

                # Check if the task succeeded
                try:
                    _, x_done = try_getting_result(  # TODO: Consider removing the first return value
                            done_future,
                            task_key=in_process.pop(done_future),
                            graph=graph
                            )
                except FailedTaskError as e:
                    exceptions.append(e)
                    if error_handling == 'immediate':
                        break
                    else:
                        continue

                if show_progress:
                    progressbars[x_done[0]].update()

                # Remove node from graph
                ys = graph.pop_with_new_leaves(x_done)

                # Update standby
                for channels, task in ys.items():
                    standby[channels].extend(task)

    if exceptions:
        raise generate_task_group(exceptions)

    # Sanity check
    assert graph.size == 0, f'Graph is not empty. Should not happen.'
    assert all(len(occ) == 0 for occ in occupation.values()), 'Incorrect task count. Should not happen.'
    return info


class FailedTaskError(Exception):
    def __init__(self, task: TaskWorkerProtocol, msg: str):
        super().__init__(msg)
        self.task = task
        self.msg = msg


def generate_task_group(errors: list[FailedTaskError]) -> ExceptionGroup:
    error_count = dict(Counter([e.task.to_tuple()[0] for e in errors]))
    task_groups: dict[str, list[FailedTaskError]] = defaultdict(list)
    for e in errors:
        k = e.task.to_tuple()[0]
        task_groups[k].append(e)

    exception_groups = {k: ExceptionGroup(f'{len(v)} task(s) failed in {k}', v) for k, v in task_groups.items()}
    out = ExceptionGroup(
            f'Failed task count: {error_count}',
            list(exception_groups.values())
            )
    return out


def try_getting_result(future: Future[tuple[TaskLabels, TaskKey]], task_key: TaskKey, graph: TaskGraph) -> tuple[TaskLabels, TaskKey]:
    try:
        return future.result()
    except Exception as e:
        task = graph.get_task(task_key)
        raise FailedTaskError(task, msg=task.dump_error_msg()) from e


@dataclass
class _TaskRunner:
    channels: TaskLabels
    task_data: bytes
    on_child_process: bool
    interactive: bool
    prefix_command: str | None

    def __call__(self) -> tuple[TaskLabels, TaskKey]:
        task = cloudpickle.loads(self.task_data)
        assert isinstance(task, TaskWorkerProtocol)
        task.set_result(
                on_child_process=self.on_child_process,
                interactive=self.interactive,
                prefix_command=self.prefix_command,
                )
        return self.channels, task.to_tuple()


def _get_prefix_command(channels: TaskLabels, prefixes: dict[str, str]) -> str | None:
    hit = [(chan, prefixes[chan]) for chan in channels if chan in prefixes]
    if not hit:
        return None
    else:
        if len(hit) > 1:
            LOGGER.warn(f'Multiple prefixes hit: {channels=}, {hit=}. Using the first.')
        _, p = hit[0]
        return p
