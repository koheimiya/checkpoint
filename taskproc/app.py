from __future__ import annotations
from typing import Any
from pathlib import Path
import sys
import json
import logging
import os

import click
import dotenv

from taskproc.graph import FailedTaskError

from .types import Context
from .task import TaskBase


LOGGER = logging.getLogger(__name__)


@click.command
@click.argument('taskfile', type=Path)
@click.option('-e', '--entrypoint', default='Main', help='Task name for entrypoint.')
@click.option('-t', '--exec-type', type=click.Choice(['process', 'thread']), default=None)
@click.option('-w', '--max-workers', type=int, default=None)
@click.option('--kwargs', type=json.loads, default=None, help='Parameters of entrypoint.')
@click.option('--cache-dir', type=Path, default=None, help='Change cache directory. Default to taskfile.parent / ".cache".')
@click.option('--rate-limits', type=json.loads, default=None, help='JSON dictionary for rate_limits.')
@click.option('-D', '--disable-detect-source-change', is_flag=True, help='Disable automatic source change detection based on AST.')
@click.option('--dont-force-entrypoint', is_flag=True, help='Do nothing if the cache of the entripoint task is up-to-date.')
@click.option('-l', '--loglevel', type=click.Choice(['debug', 'info', 'warning', 'error']), default='warning')
@click.option('--dont-show-progress', is_flag=True)
def main(taskfile: Path,
         entrypoint: str,
         exec_type: str | None,
         max_workers: int | None,
         kwargs: dict[str, Any] | None,
         cache_dir: Path | None,
         rate_limits: dict[str, Any] | None,
         disable_detect_source_change: bool,
         dont_force_entrypoint: bool,
         loglevel: str,
         dont_show_progress: bool,
         ) -> int:
    logging.basicConfig(level=getattr(logging, loglevel.upper()))
    LOGGER.info('Entering main of taskproc.')

    if exec_type is not None:
        Context.executor_name = exec_type
    if max_workers is not None:
        Context.max_workers = max_workers
    if cache_dir is not None:
        Context.cache_dir = cache_dir
    Context.detect_source_change = not disable_detect_source_change

    if Context.get_envfile_path():
        LOGGER.info('Loaded envfile at ' + Context.get_envfile_path())
    Context.log_settings()

    if kwargs is None:
        kwargs = {}
    assert isinstance(kwargs, dict)

    # Run script as module
    module_name = taskfile.with_suffix('').name
    sys.path.append(str(taskfile.parent))
    pp = os.getenv('PYTHONPATH')
    if pp is not None:
        os.environ['PYTHONPATH'] = ':'.join([str(taskfile.parent), pp])
    else:
        os.environ['PYTHONPATH'] = str(taskfile.parent)
    module = __import__(module_name)
    LOGGER.info(f'Target module {module_name} loaded.')

    # Run the main task
    entrypoint_fn = getattr(module, entrypoint)
    assert issubclass(entrypoint_fn, TaskBase), \
            f'Taskfile `{taskfile}` should contain a task(factory) `{entrypoint}`, but found `{entrypoint_fn}`.'
    entrypoint_task = entrypoint_fn(**kwargs)
    if not dont_force_entrypoint:
        entrypoint_task.clear_task()
    try:
        _, stats = entrypoint_task.run_graph_with_stats(rate_limits=rate_limits, show_progress=not dont_show_progress)
    except FailedTaskError as e:
        os.system('stty sane')  # Fix broken tty after Popen with tricky command. Need some fix in the future.
        e.task.log_error()
        raise

    LOGGER.debug(f"stats:\n{stats}")

    os.system('stty sane')  # Fix broken tty after Popen with tricky command. Need some fix in the future.
    if entrypoint_task.task_stdout.exists():
        print("==== ENTRYPOINT STDOUT (DETACHED) ====")
        print(open(entrypoint_task.task_stdout).read())
    else:
        print("==== NO ENTRYPOINT STDOUT (DETACHED) ====")

    if entrypoint_task.task_stderr.exists():
        print("==== ENTRYPOINT STDERR (DETACHED) ====")
        print(open(entrypoint_task.task_stderr).read())
    else:
        print("==== NO ENTRYPOINT STDERR (DETACHED) ====")
    return 0


if __name__ == '__main__':
    sys.exit(main())
