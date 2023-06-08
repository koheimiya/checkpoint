""" A lightweight workflow management tool written in pure Python.

Key features:
    - Intuitive and flexible task graph creation with small boilerblates.
    - Automatic cache/data management (source code change detection, cache/data dependency tracking).
    - Task queue with rate limits.

Limitations:
    - No priority-based scheduling.
"""
from .types import Context
from .task import Task, TaskBase, Req, Requires, RequiresList, RequiresDict, Const
from .graph import FailedTaskError


__EXPORT__ = [
        Task, Const, TaskBase,
        Req, Requires, RequiresList, RequiresDict,
        Context,
        FailedTaskError
        ]
