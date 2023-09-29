from __future__ import annotations
from typing_extensions import NewType


JsonDict = NewType('JsonDict', dict)
JsonStr = NewType('JsonStr', str)
TaskKey = tuple[str, JsonStr]
