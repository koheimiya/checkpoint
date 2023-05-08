""" A lightweight workflow management tool written in pure Python.

Key features:
    - Intuitive and flexible task graph creation with small boilerblates.
    - Automatic cache/data management (source code change detection, cache/data dependency tracking).
    - Task queue with rate limits.

Limitations:
    - No priority-based scheduling.
"""
from .types import Context
from .task import infer_task_type, Task, TaskLike, Req, Requires, RequiresList, RequiresDict, Const, DataPath


__EXPORT__ = [
        infer_task_type, Task, Req, Requires, RequiresList, RequiresDict, Const, DataPath, TaskLike,
        Context
        ]
