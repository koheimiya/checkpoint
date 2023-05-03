""" A lightweight workflow management tool written in pure Python.

Key features:
    - Intuitive and flexible task graph creation with small boilerblates.
    - Automatic cache/data management (source code change detection, cache/data dependency tracking).
    - Task queue with rate limits.

Limitations:
    - No priority-based scheduling.
"""
import sys

from .app import main
from .types import Context
from .task import task, Requires, RequiresDirectory, RequiresDict, RequiresList


__EXPORT__ = [task, Requires, RequiresList, RequiresDict, RequiresDirectory, Context]


if __name__ == '__main__':
    sys.exit(main())
