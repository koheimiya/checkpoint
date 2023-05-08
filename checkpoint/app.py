from __future__ import annotations
from typing import Any
from pathlib import Path
import sys
import json

import click

from .types import Context
from .task import TaskType


@click.command
@click.argument('taskfile', type=Path)
@click.option('-e', '--entrypoint', default='Main', help='Task name for entrypoint.')
@click.option('-t', '--exec-type', type=click.Choice(['process', 'thread']), default='process')
@click.option('-w', '--max-workers', type=int, default=-1)
@click.option('--cache-dir', type=Path, default=None)
@click.option('--rate-limits', type=json.loads, default=None, help='JSON dictionary for rate_limits.')
@click.option('-D', '--detect-source-change', is_flag=True, help='Automatically discard the cache per task once the source code (AST) is changed.')
def main(taskfile: Path, entrypoint: str, exec_type: str, max_workers: int, cache_dir: Path | None, rate_limits: dict[str, Any] | None, detect_source_change: bool):
    # Set arguments as environment variables
    # os.environ['CP_EXECUTOR'] = exec_type
    # os.environ['CP_MAX_WORKERS'] = str(max_workers)
    # os.environ['CP_CACHE_DIR'] = str(taskfile.parent / '.cache') if cache_dir is None else str(cache_dir)
    # os.environ['CP_DETECT_SOURCE_CHANGE'] = str(int(detect_source_change))
    Context.executor_name = exec_type
    Context.max_workers = max_workers
    Context.cache_dir = taskfile.parent / '.cache' if cache_dir is None else cache_dir
    Context.detect_source_change = detect_source_change

    # Run script as module
    module_name = taskfile.with_suffix('').name
    sys.path.append(str(taskfile.parent))
    module = __import__(module_name)
    # import importlib.util
    # spec = importlib.util.spec_from_file_location(module_name, taskfile)
    # assert spec is not None
    # assert spec.loader is not None
    # module = importlib.util.module_from_spec(spec)
    # sys.modules[module_name] = module
    # spec.loader.exec_module(module)

    # Run the main task
    entrypoint_fn = getattr(module, entrypoint)
    assert issubclass(entrypoint_fn, TaskType), \
            f'Taskfile `{taskfile}` should contain a task(factory) `{entrypoint}`, but found `{entrypoint_fn}`.'
    task = entrypoint_fn()
    task.run_task_with_stats(rate_limits=rate_limits)
    return 0


if __name__ == '__main__':
    sys.exit(main())
