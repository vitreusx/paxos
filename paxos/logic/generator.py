import time
from abc import ABC, abstractmethod, abstractproperty
from typing import Any


class IDGenerator(ABC):
    """Object that is able to generate unique ids
    (also among other generators if set up properly)"""

    @abstractmethod
    def next_id(self) -> int:
        """return next unique id"""

    @abstractproperty
    def state(self) -> Any:
        """for serializing purposes"""

    @state.setter
    def state(self, value: Any) -> None:
        """recover from the given state value"""


class IncrementalIDGenerator(IDGenerator):
    def __init__(self, uid: int, max_uid: int):
        assert 0 <= uid <= max_uid
        self.next = uid
        self.uid_pool = max_uid + 1

    def next_id(self) -> int:
        id = self.next
        self.next += self.uid_pool
        return id

    @property
    def state(self) -> int:
        return self.next

    @state.setter
    def state(self, value: int) -> None:
        self.next = value


class TimeAwareIDGenerator(IDGenerator):
    def __init__(self, uid: int, max_uid: int):
        assert 0 <= uid <= max_uid
        self.uid = uid
        self.uid_pool = max_uid + 1

    def next_id(self) -> int:
        now = round(time.time() * 1000)
        return now * self.uid_pool + self.uid

    @property
    def state(self) -> None:
        return None

    @state.setter
    def state(self, value: None) -> None:
        pass
