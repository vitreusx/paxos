from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from paxos.logic.dictionary import WriteOnceDict


class StateMachine(ABC):
    """A distributed state machine."""

    def __init__(self, paxos: WriteOnceDict, prefix: str, init_state):
        self.paxos = paxos
        self.prefix = prefix
        self.watermark = 0
        self.state = init_state

    @abstractmethod
    async def apply(self, command):
        ...

    async def sync(self):
        while True:
            ins_value = await self.paxos[self.prefix, self.watermark]
            if ins_value is not None:
                _, ins_cmd = ins_value
                await self.apply(ins_cmd)
                self.watermark += 1
            else:
                break

    async def execute(self, command):
        await self.sync()

        while True:
            key = self.prefix, self.watermark
            successful, ins_cmd = await self.paxos.set(key, command)
            ret_val = await self.apply(ins_cmd)
            self.watermark += 1
            if successful:
                return ret_val


class PaxosVar(StateMachine):
    """A Paxos-managed variable - as opposed to the value reached by consensus through ordinary Paxos protocol, it can be changed."""

    @dataclass
    class SetValue:
        new_value: Any

    def __init__(self, paxos: WriteOnceDict, prefix: str, init_value: Any):
        super().__init__(paxos, prefix, init_value)

    async def apply(self, command):
        assert isinstance(command, PaxosVar.SetValue)
        self.state = command.new_value

    async def set(self, new_value: Any):
        cmd = PaxosVar.SetValue(new_value)
        return await self.execute(cmd)

    async def get(self):
        await self.sync()
        return self.state
