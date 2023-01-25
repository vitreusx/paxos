from abc import ABC, abstractmethod
from dataclasses import dataclass
from logging import Logger
from typing import Any

from paxos.logic.dictionary import WriteOnceDict


class StateMachine(ABC):
    """A distributed state machine."""

    def __init__(
        self, paxos: WriteOnceDict, prefix: str, init_state: Any, logger: Logger
    ):
        self.paxos = paxos
        self.prefix = prefix
        self.watermark = 0
        self.current_state = init_state
        self.logger = logger

    @abstractmethod
    async def apply(self, command):
        ...

    async def sync(self):
        while True:
            self.logger.critical(f"syncing {self.prefix=}, {self.watermark=}")
            ins_cmd = await self.paxos[self.prefix, self.watermark]
            if ins_cmd is not None:
                await self.apply(ins_cmd)
                self.watermark += 1
            else:
                break

    async def execute(self, command):
        await self.sync()

        while True:
            key = self.prefix, self.watermark
            applied_cmd = await self.paxos.set(key, command)
            ret_val = await self.apply(applied_cmd)
            self.watermark += 1
            if applied_cmd == command:
                return ret_val

    @property
    def state(self) -> tuple[int, Any, Any]:
        return self.watermark, self.state, self.paxos.state

    @state.setter
    def state(self, value: tuple[int, Any, Any]):
        self.watermark, self.state, self.paxos.state = value


class PaxosVar(StateMachine):
    """A Paxos-managed variable - as opposed to the value reached by consensus through ordinary Paxos protocol, it can be changed."""

    @dataclass
    class SetValue:
        new_value: Any

    def __init__(self, paxos: WriteOnceDict, prefix: str, init_value: Any):
        super().__init__(paxos, prefix, init_value)

    async def apply(self, command):
        assert isinstance(command, PaxosVar.SetValue)
        self.current_state = command.new_value

    async def set(self, new_value: Any):
        cmd = PaxosVar.SetValue(new_value)
        return await self.execute(cmd)

    async def get(self):
        await self.sync()
        return self.current_state
