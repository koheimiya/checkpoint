""" A lightweight workflow management tool written in pure Python.

Key features:
    - Intuitive and flexible task graph creation with small boilerblates.
    - Automatic cache/data management (source code change detection, cache/data dependency tracking).
    - Task queue with rate limits.

Limitations:
    - No priority-based scheduling.
"""
from .future import Future
from .task import Task, Req, Requires, RequiresList, RequiresDict, Const, Cache
from .graph import FailedTaskError


__EXPORT__ = [
        Future, Const, Task,
        Req, Requires, RequiresList, RequiresDict,
        Cache,
        FailedTaskError
        ]
