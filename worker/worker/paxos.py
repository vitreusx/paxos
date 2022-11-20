from typing import TypeVar, Callable, Generic

__all__ = ["Paxos"]


T = TypeVar("T")


class Assign(Generic[T]):
    def __init__(self, new_value: T):
        self.new_value = new_value

    def __call__(self, cur_value: T) -> T:
        return self.new_value


State = TypeVar("State")
Transition = Callable[[State], State]


class Paxos(Generic[State]):
    def __init__(self, init_state: State):
        self._s = init_state

    @property
    def state(self) -> State:
        return self._s

    @state.setter
    def state(self, new_value: State):
        self.apply(f=Assign(new_value))

    def apply(self, f: Transition):
        pass
