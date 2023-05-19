""" DAG processor """
from __future__ import annotations
from datetime import datetime
from typing import Any, Sequence
from typing_extensions import Self, runtime_checkable, Protocol
from collections import defaultdict
from dataclasses import dataclass
from concurrent.futures import Future, wait, FIRST_COMPLETED, Executor
import logging

from tqdm.auto import tqdm
import cloudpickle
import networkx as nx

from .types import Json, TaskKey


LOGGER = logging.getLogger(__name__)


ChannelLabels = tuple[str, ...]


@runtime_checkable
class TaskHandlerProtocol(Protocol):
    @property
    def channels(self) -> ChannelLabels: ...
    @property
    def source_timestamp(self) -> datetime: ...
    def to_tuple(self) -> TaskKey: ...
    def get_prerequisites(self) -> Sequence[TaskHandlerProtocol]: ...
    def peek_timestamp(self) -> datetime | None: ...
    def set_result(self) -> None: ...


@dataclass
class TaskGraph:
    G: nx.DiGraph
    detect_source_change: bool

    @classmethod
    def build_from(cls, root: TaskHandlerProtocol, detect_source_change: bool) -> Self:
        G = nx.DiGraph()
        seen: set[TaskKey] = set()
        to_expand = [root]
        while to_expand:
            task = to_expand.pop()
            x = task.to_tuple()
            if x not in seen:
                seen.add(x)
                prerequisite_tasks = task.get_prerequisites()
                to_expand.extend(prerequisite_tasks)
                G.add_node(x, task=task, timestamp=task.peek_timestamp(), source_timestamp=task.source_timestamp)
                G.add_edges_from([(p.to_tuple(), x) for p in prerequisite_tasks])
        out = TaskGraph(G, detect_source_change=detect_source_change)
        out.trim()
        return out

    @property
    def size(self) -> int:
        return len(self.G)

    def get_task(self, key: TaskKey) -> TaskHandlerProtocol:
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

    def get_initial_tasks(self) -> dict[ChannelLabels, list[TaskKey]]:
        leaves = [x for x in self.G if self.G.in_degree(x) == 0]
        return self._group_by_channels(leaves)

    def _group_by_channels(self, nodes: list[TaskKey]) -> dict[ChannelLabels, list[TaskKey]]:
        out = defaultdict(list)
        for x in nodes:
            out[self.get_task(x).channels].append(x)
        return out

    def pop_with_new_leaves(self, x: TaskKey, disallow_non_leaf: bool = True) -> dict[ChannelLabels, list[TaskKey]]:
        if disallow_non_leaf:
            assert not list(self.G.predecessors(x))

        new_leaves: list[TaskKey] = []
        for y in self.G.successors(x):
            if self.G.in_degree(y) == 1:
                new_leaves.append(y)

        self.G.remove_node(x)
        return self._group_by_channels(new_leaves)

    def get_nodes_by_task(self) -> dict[str, list[Json]]:
        out: dict[str, list[Json]] = defaultdict(list)
        for x in self.G:
            path, args = x
            out[path].append(args)
        return dict(out)


def run_task_graph(
        graph: TaskGraph,
        executor: Executor,
        rate_limits: dict[str, int] | None = None,
        dump_graphs: bool = False,
        show_progress: bool = False,
        ) -> dict[str, Any]:
    """ Consume task graph concurrently.
    """
    stats = {k: len(args) for k, args in graph.get_nodes_by_task().items()}
    LOGGER.debug(f'Following tasks will be called: {stats}')
    info = {'stats': stats, 'generations': []}

    if show_progress:
        progressbars = {k: tqdm(range(n), desc=k, position=i) for i, (k, n) in enumerate(stats.items())}
    else:
        progressbars = {}

    # Read concurrency budgets
    if rate_limits is None:
        rate_limits = {}
    occupied = {k: 0 for k in rate_limits}

    # Execute tasks
    standby = graph.get_initial_tasks()
    in_process: set[Future[tuple[ChannelLabels, TaskKey]]] = set()
    with executor as executor:
        while standby or in_process:
            # Log some stats
            LOGGER.debug(
                    f'nodes: {graph.size}, '
                    f'standby: {len(standby)}, '
                    f'in_process: {len(in_process)}'
                    )
            if dump_graphs:
                info['generations'].append(graph.get_nodes_by_task())

            # Submit all leaf tasks
            leftover: dict[ChannelLabels, list[TaskKey]] = {}
            for queue, keys in standby.items():
                if any(q in rate_limits for q in queue):
                    free = min(rate_limits[q] - occupied[q] for q in queue if q in rate_limits)
                    to_submit, to_hold = keys[:free], keys[free:]
                    for q in queue:
                        if q in occupied:
                            occupied[q] += len(to_submit)
                    if to_hold:
                        leftover[queue] = to_hold
                else:
                    to_submit = keys

                for key in to_submit:
                    future = executor.submit(_run_task, queue, cloudpickle.dumps(graph.get_task(key)))
                    in_process.add(future)

            # Wait for the first tasks to complete
            done, in_process = wait(in_process, return_when=FIRST_COMPLETED)

            # Update graph
            standby = defaultdict(list, leftover)
            for done_future in done:
                queue_done, x_done = done_future.result()
                if show_progress:
                    progressbars[x_done[0]].update()

                # Update occupied
                for q in queue_done:
                    if q in occupied:
                        occupied[q] -= 1
                        assert occupied[q] >= 0

                # Remove node from graph
                ys = graph.pop_with_new_leaves(x_done)

                # Update standby
                for queue, task in ys.items():
                    standby[queue].extend(task)

    for pbar in progressbars.values():
        pbar.close()

    # Sanity check
    assert graph.size == 0, f'Graph is not empty. Should not happen.'
    assert all(n == 0 for n in occupied.values()), 'Incorrect task count. Should not happen.'
    return info


def _run_task(queue: ChannelLabels, task_data: bytes) -> tuple[ChannelLabels, TaskKey]:  # queue, (dbname, key)
    task = cloudpickle.loads(task_data)
    assert isinstance(task, TaskHandlerProtocol)
    task.set_result()
    return queue, task.to_tuple()
