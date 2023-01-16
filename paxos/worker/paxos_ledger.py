from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from paxos.logic.types import MultiPaxos, StateMachine
from paxos.worker.ledger import Account, Decimal, Ledger


class LedgerCmd(ABC):
    @abstractmethod
    def apply(self, ledger: Ledger) -> Any:
        ...


@dataclass
class OpenAccount(LedgerCmd):
    def apply(self, ledger: Ledger):
        return ledger.open_acct()


@dataclass
class Deposit(LedgerCmd):
    uid: int
    amount: Decimal

    def apply(self, ledger: Ledger):
        return ledger.deposit(self.uid, self.amount)


@dataclass
class Withdraw(LedgerCmd):
    uid: int
    amount: Decimal

    def apply(self, ledger: Ledger):
        return ledger.withdraw(self.uid, self.amount)


@dataclass
class Transfer(LedgerCmd):
    from_uid: int
    to_uid: int
    amount: Decimal

    def apply(self, ledger: Ledger):
        return ledger.transfer(self.from_uid, self.to_uid, self.amount)


class PaxosLedger(StateMachine):
    def __init__(
        self,
        paxos: MultiPaxos,
        prefix: str,
    ):
        ledger = Ledger.empty()
        super().__init__(paxos, prefix, ledger)

    async def apply(self, cmd):
        assert isinstance(self.state, Ledger)
        assert isinstance(cmd, LedgerCmd)
        return cmd.apply(self.state)

    async def open_acct(self):
        cmd = OpenAccount()
        return await self.execute(cmd)

    async def account(self, id: int) -> Account:
        await self.sync()
        return self.state.account(id)

    async def deposit(self, uid: int, amount: Decimal):
        cmd = Deposit(uid, amount)
        return await self.execute(cmd)

    async def withdraw(self, uid: int, amount: Decimal):
        cmd = Withdraw(uid, amount)
        return await self.execute(cmd)

    async def transfer(self, from_uid: int, to_uid: int, amount: Decimal):
        cmd = Transfer(from_uid, to_uid, amount)
        return await self.execute(cmd)
