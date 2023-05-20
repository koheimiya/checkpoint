from __future__ import annotations
from typing import Any
from pathlib import Path
import sys
import json
import pprint
import logging
import io

import click

from .types import Context
from .task import TaskType


LOGGER = logging.getLogger(__name__)


@click.command
@click.argument('taskfile', type=Path)
@click.option('-e', '--entrypoint', default='Main', help='Task name for entrypoint.')
@click.option('-t', '--exec-type', type=click.Choice(['process', 'thread']), default='process')
@click.option('-w', '--max-workers', type=int, default=-1)
@click.option('--cache-dir', type=Path, default=None)
@click.option('--rate-limits', type=json.loads, default=None, help='JSON dictionary for rate_limits.')
@click.option('-D', '--detect-source-change', is_flag=True, help='Automatically discard the cache per task once the source code (AST) is changed.')
@click.option('--dont-force-entrypoint', is_flag=True, help='Do nothing if the cache of the entripoint task is up-to-date.')
@click.option('-l', '--loglevel', type=click.Choice(['debug', 'info', 'warning', 'error']), default='warning')
@click.option('--dont-show-progress', is_flag=True)
def main(taskfile: Path,
         entrypoint: str,
         exec_type: str,
         max_workers: int,
         cache_dir: Path | None,
         rate_limits: dict[str, Any] | None,
         detect_source_change: bool,
         dont_force_entrypoint: bool,
         loglevel: str,
         dont_show_progress: bool,
         ) -> int:
    logging.basicConfig(level=getattr(logging, loglevel.upper()))

    Context.executor_name = exec_type
    Context.max_workers = max_workers
    Context.cache_dir = taskfile.parent / '.cache' if cache_dir is None else cache_dir
    Context.detect_source_change = detect_source_change

    # Run script as module
    module_name = taskfile.with_suffix('').name
    sys.path.append(str(taskfile.parent))
    module = __import__(module_name)

    # Run the main task
    entrypoint_fn = getattr(module, entrypoint)
    assert issubclass(entrypoint_fn, TaskType), \
            f'Taskfile `{taskfile}` should contain a task(factory) `{entrypoint}`, but found `{entrypoint_fn}`.'
    entrypoint_task = entrypoint_fn()
    if not dont_force_entrypoint:
        entrypoint_task.clear_task()
    _, stats = entrypoint_task.run_graph_with_stats(rate_limits=rate_limits, show_progress=not dont_show_progress)

    # LOGGER.info('Execution summary:')
    # buf = io.StringIO()
    # pprint.pprint(stats['stats'], sort_dicts=False, stream=buf)
    # for line in buf.getvalue().splitlines():
    #     LOGGER.info(line)
    print('==== STDOUT ====')
    for line in open(entrypoint_task.task_stdout).readlines():
        print(line, end='')
    print('==== STDERR ====')
    for line in open(entrypoint_task.task_stderr).readlines():
        print(line, end='')
    return 0


if __name__ == '__main__':
    sys.exit(main())
