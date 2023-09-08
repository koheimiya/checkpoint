from __future__ import annotations
import argparse
from typing import Any, Sequence, Type
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


def run_with_args(
        task_class: Type[TaskBase[Any]],
        args: Sequence[str] | None = None,
        defaults: argparse.Namespace | None = None,
        ) -> None:
    if defaults is None:
        params = argparse.Namespace()
    else:
        params = defaults
    parser = argparse.ArgumentParser()
    parser.add_argument('result_dir', type=Path, help='Path to result directory.')       
    parser.add_argument('-e', '--entrypoint', default='Main', help='Task name for entrypoint.')                                              
    parser.add_argument('-t', '--exec-type', choices=['process', 'thread'], default=None)                                         
    parser.add_argument('-w', '--max-workers', type=int, default=None)                                                                       
    parser.add_argument('-i', '--interactive', action='store_true')                                                                                 
    parser.add_argument('-D', '--disable-detect-source-change', action='store_true', help='Disable automatic source change detection based on AST.')
    parser.add_argument('-l', '--loglevel', choices=['debug', 'info', 'warning', 'error'], default='warning')                     
    parser.add_argument('--kwargs', type=json.loads, default=None, help='Parameters of entrypoint.')                                         
    parser.add_argument('--rate-limits', type=json.loads, default=None, help='JSON dictionary for rate_limits.')                             
    parser.add_argument('--dont-force-entrypoint', action='store_true', help='Do nothing if the cache of the entripoint task is up-to-date.')       
    parser.add_argument('--dont-show-progress', action='store_true')                                                                                
    parser.add_argument('--prefix', type=json.loads, default=None, help='Prefix commands per channels.')                                                                                
    parser.parse_args(args=args, namespace=params)

    logging.basicConfig(level=getattr(logging, params.loglevel.upper()))
    LOGGER.info('Parsing args from CLI.')
    LOGGER.info(f'Params: {params}')

    Context.cache_dir = params.result_dir
    task_instance = task_class(**(params.kwargs if params.kwargs is not None else {}))
    if not params.dont_force_entrypoint:
        task_instance.clear_task()
    try:
        _, stats = task_instance.run_graph(
                executor=Context.get_executor(params.exec_type, max_workers=params.max_workers),
                rate_limits=params.rate_limits,
                detect_source_change=not params.disable_detect_source_change,
                show_progress=not params.dont_show_progress,
                force_interactive=params.interactive,
                prefixes=params.prefix,
                )
    except FailedTaskError as e:
        os.system('stty sane')  # Fix broken tty after Popen with tricky command. Need some fix in the future.
        e.task.log_error()
        raise
    else:
        os.system('stty sane')

    LOGGER.debug(f"stats:\n{stats}")

    if task_instance.task_stdout.exists():
        print("==== ENTRYPOINT STDOUT (DETACHED) ====")
        print(open(task_instance.task_stdout).read())
    else:
        print("==== NO ENTRYPOINT STDOUT (DETACHED) ====")

    if task_instance.task_stderr.exists():
        print("==== ENTRYPOINT STDERR (DETACHED) ====")
        print(open(task_instance.task_stderr).read())
    else:
        print("==== NO ENTRYPOINT STDERR (DETACHED) ====")


# @click.command
# @click.argument('taskfile', type=Path)
# @click.option('-e', '--entrypoint', default='Main', help='Task name for entrypoint.')
# @click.option('-t', '--exec-type', type=click.Choice(['process', 'thread']), default=None)
# @click.option('-w', '--max-workers', type=int, default=None)
# @click.option('-i', '--interactive', is_flag=True)
# @click.option('--kwargs', type=json.loads, default=None, help='Parameters of entrypoint.')
# @click.option('--cache-dir', type=Path, default=None, help='Change cache directory. Default to taskfile.parent / ".cache".')
# @click.option('--rate-limits', type=json.loads, default=None, help='JSON dictionary for rate_limits.')
# @click.option('-D', '--disable-detect-source-change', is_flag=True, help='Disable automatic source change detection based on AST.')
# @click.option('--dont-force-entrypoint', is_flag=True, help='Do nothing if the cache of the entripoint task is up-to-date.')
# @click.option('-l', '--loglevel', type=click.Choice(['debug', 'info', 'warning', 'error']), default='warning')
# @click.option('--dont-show-progress', is_flag=True)
# def main(taskfile: Path,
#          entrypoint: str,
#          exec_type: str | None,
#          max_workers: int | None,
#          interactive: bool,
#          kwargs: dict[str, Any] | None,
#          cache_dir: Path | None,
#          rate_limits: dict[str, Any] | None,
#          disable_detect_source_change: bool,
#          dont_force_entrypoint: bool,
#          loglevel: str,
#          dont_show_progress: bool,
#          ) -> int:
#     logging.basicConfig(level=getattr(logging, loglevel.upper()))
#     LOGGER.info('Entering main of taskproc.')
# 
#     if exec_type is not None:
#         Context.executor_name = exec_type
#     if max_workers is not None:
#         Context.max_workers = max_workers
#     if cache_dir is not None:
#         Context.cache_dir = cache_dir
#     Context.detect_source_change = not disable_detect_source_change
#     Context.interactive = interactive
# 
#     if Context.get_envfile_path():
#         LOGGER.info('Loaded envfile at ' + Context.get_envfile_path())
#     Context.log_settings()
# 
#     if kwargs is None:
#         kwargs = {}
#     assert isinstance(kwargs, dict)
# 
#     # Run script as module
#     module_name = taskfile.with_suffix('').name
#     sys.path.append(str(taskfile.parent))
#     pp = os.getenv('PYTHONPATH')
#     if pp is not None:
#         os.environ['PYTHONPATH'] = ':'.join([str(taskfile.parent), pp])
#     else:
#         os.environ['PYTHONPATH'] = str(taskfile.parent)
#     module = __import__(module_name)
#     LOGGER.info(f'Target module {module_name} loaded.')
# 
#     # Run the main task
#     entrypoint_fn = getattr(module, entrypoint)
#     assert issubclass(entrypoint_fn, TaskBase), \
#             f'Taskfile `{taskfile}` should contain a task(factory) `{entrypoint}`, but found `{entrypoint_fn}`.'
#     entrypoint_task = entrypoint_fn(**kwargs)
#     if not dont_force_entrypoint:
#         entrypoint_task.clear_task()
#     try:
#         _, stats = entrypoint_task.run_graph(rate_limits=rate_limits, show_progress=not dont_show_progress, force_interactive=Context.interactive)
#     except FailedTaskError as e:
#         os.system('stty sane')  # Fix broken tty after Popen with tricky command. Need some fix in the future.
#         e.task.log_error()
#         raise
# 
#     LOGGER.debug(f"stats:\n{stats}")
# 
#     os.system('stty sane')  # Fix broken tty after Popen with tricky command. Need some fix in the future.
#     if entrypoint_task.task_stdout.exists():
#         print("==== ENTRYPOINT STDOUT (DETACHED) ====")
#         print(open(entrypoint_task.task_stdout).read())
#     else:
#         print("==== NO ENTRYPOINT STDOUT (DETACHED) ====")
# 
#     if entrypoint_task.task_stderr.exists():
#         print("==== ENTRYPOINT STDERR (DETACHED) ====")
#         print(open(entrypoint_task.task_stderr).read())
#     else:
#         print("==== NO ENTRYPOINT STDERR (DETACHED) ====")
#     return 0
