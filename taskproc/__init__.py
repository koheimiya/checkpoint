""" A lightweight workflow management tool written in pure Python.

Key features:
    - Intuitive and flexible task graph creation with small boilerblates.
    - Automatic cache/data management (source code change detection, cache/data dependency tracking).
    - Task queue with rate limits.

Limitations:
    - No priority-based scheduling.
"""
from .future import Future, Const, FutureList, FutureDict
from .task import Task, Cache
from .graph import FailedTaskError


__EXPORT__ = [
        Future, Const, FutureList, FutureDict,
        Task,
        Cache,
        FailedTaskError
        ]
