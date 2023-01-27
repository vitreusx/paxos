from paxos.ledger.base import (
    Account,
    Decimal,
    Deposit,
    Ledger,
    LedgerCmd,
    OpenAccount,
    Transfer,
    Withdraw,
)
from paxos.ledger.file import FileLedger
from paxos.logic.multi import MultiPaxos
from paxos.logic.types import StateMachine
from typing import Union
from pathlib import Path


class PaxosLedger(StateMachine):
    def __init__(self, paxos: MultiPaxos, prefix: str, ledger_path: Union[str, Path]):
        ledger = FileLedger(ledger_path)
        super().__init__(paxos, prefix, ledger)

    async def apply(self, cmd: LedgerCmd):
        ledger: FileLedger = self.state

        if isinstance(cmd, OpenAccount):
            return ledger.open_acct()
        elif isinstance(cmd, Deposit):
            return ledger.deposit(cmd.uid, cmd.amount)
        elif isinstance(cmd, Withdraw):
            return ledger.withdraw(cmd.uid, cmd.amount)
        elif isinstance(cmd, Transfer):
            return ledger.transfer(cmd.from_uid, cmd.to_uid, cmd.amount)
        else:
            raise ValueError(f"Unknown command {cmd}")

    async def open_acct(self):
        cmd = OpenAccount()
        return await self.execute(cmd)

    async def account(self, id: int) -> Account:
        await self.sync()
        ledger: FileLedger = self.state
        return ledger.account(id)

    async def deposit(self, uid: int, amount: Decimal):
        cmd = Deposit(uid, amount)
        return await self.execute(cmd)

    async def withdraw(self, uid: int, amount: Decimal):
        cmd = Withdraw(uid, amount)
        return await self.execute(cmd)

    async def transfer(self, from_uid: int, to_uid: int, amount: Decimal):
        cmd = Transfer(from_uid, to_uid, amount)
        return await self.execute(cmd)
