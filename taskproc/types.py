from __future__ import annotations
from typing_extensions import NewType


Json = NewType('Json', str)
TaskKey = tuple[str, Json]
