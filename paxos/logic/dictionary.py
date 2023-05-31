from abc import ABC, abstractmethod
from typing import Any


class WriteOnceDict(ABC):
    """A dictionary of sorts, with each key being assigned a writeable-once value."""

    @abstractmethod
    async def set(self, key: Any, value: Any) -> Any:
        """Propose a value to be associated with a given key.
        Returns the final value reached by consensus (which may or may not be the proposed value)."""

    @abstractmethod
    async def __getitem__(self, key: Any) -> Any:
        """Get the value associated with a given key.
        If consensus has not yet been reached on what should be the value, None is returned."""
