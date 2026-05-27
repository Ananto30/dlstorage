from typing import Protocol, runtime_checkable

from dlstorage.types import Message


@runtime_checkable
class ConnectionPool(Protocol):
    """Protocol for synchronous connection pools."""

    def execute(self, host: str, port: int, msg: Message) -> Message | None: ...
    def close_peer(self, host: str, port: int) -> None: ...
    def close_all(self) -> None: ...


@runtime_checkable
class AsyncConnectionPool(Protocol):
    """Protocol for async connection pools."""

    async def execute(self, host: str, port: int, msg: Message) -> Message | None: ...
    def close_peer(self, host: str, port: int) -> None: ...
    def close_all(self) -> None: ...
