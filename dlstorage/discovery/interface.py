from typing import Protocol, runtime_checkable
from dlstorage.types import NodeInfo


@runtime_checkable
class Discovery(Protocol):
    """Protocol every discovery backend must satisfy."""

    def get_peers(self) -> list[NodeInfo]: ...
