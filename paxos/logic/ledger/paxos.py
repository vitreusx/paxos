from logging import Logger

from paxos.logic.ledger.base import (
    Account,
    Decimal,
    Deposit,
    Ledger,
    LedgerCmd,
    OpenAccount,
    Transfer,
    Withdraw,
)
from paxos.logic.ledger.file import FileLedger
from paxos.logic.multi import MultiPaxos
from paxos.logic.types import StateMachine


class PaxosLedger(StateMachine):
    def __init__(
        self, paxos: MultiPaxos, prefix: str, parent_logger: Logger, ledger_path
    ):
        ledger = FileLedger(ledger_path)
        super().__init__(paxos, prefix, ledger, parent_logger.getChild("ledger"))

    async def apply(self, cmd: LedgerCmd):
        assert isinstance(self.current_state, Ledger)
        if isinstance(cmd, OpenAccount):
            return self.current_state.open_acct()
        elif isinstance(cmd, Deposit):
            return self.current_state.deposit(cmd.uid, cmd.amount)
        elif isinstance(cmd, Withdraw):
            return self.current_state.withdraw(cmd.uid, cmd.amount)
        elif isinstance(cmd, Transfer):
            return self.current_state.transfer(cmd.from_uid, cmd.to_uid, cmd.amount)
        else:
            raise ValueError(f"Unknown command {cmd}")

    async def open_acct(self):
        cmd = OpenAccount()
        return await self.execute(cmd)

    async def account(self, id: int) -> Account:
        await self.sync()
        return self.current_state.account(id)

    async def deposit(self, uid: int, amount: Decimal):
        cmd = Deposit(uid, amount)
        return await self.execute(cmd)

    async def withdraw(self, uid: int, amount: Decimal):
        cmd = Withdraw(uid, amount)
        return await self.execute(cmd)

    async def transfer(self, from_uid: int, to_uid: int, amount: Decimal):
        cmd = Transfer(from_uid, to_uid, amount)
        return await self.execute(cmd)
