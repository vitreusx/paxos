from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from paxos.utils.atomic import AtomicMixin, atomic


class LedgerError(Exception):
    pass


@dataclass
class Account:
    uid: int
    funds: Decimal


@dataclass
class OpenAccount:
    pass


@dataclass
class Deposit:
    uid: int
    amount: Decimal


@dataclass
class Withdraw:
    uid: int
    amount: Decimal


@dataclass
class Transfer:
    from_uid: int
    to_uid: int
    amount: Decimal


LedgerCmd = OpenAccount | Deposit | Withdraw | Transfer


@dataclass
class Ledger(AtomicMixin):
    accounts: dict[int, Account]
    next_uid: int

    def __post_init__(self):
        AtomicMixin.__init__(self)

    def commit(self):
        pass

    @staticmethod
    def empty() -> Ledger:
        return Ledger(accounts={}, next_uid=0)

    def _assign(self, value: Ledger):
        for field in self.__dataclass_fields__:
            prev_value = getattr(value, field)
            setattr(self, field, prev_value)

    def restore(self):
        if self.prev_state is not None:
            self._assign(self.prev_state)

    @atomic
    def open_acct(self):
        acct = Account(uid=self.next_uid, funds=Decimal(0))
        self.accounts[self.next_uid] = acct
        self.next_uid += 1
        return acct.uid

    @atomic
    def account(self, uid: int) -> Account:
        if uid not in self.accounts:
            raise LedgerError(f"Account with UID {uid} does not exist.")
        return self.accounts[uid]

    @atomic
    def deposit(self, uid: int, amount: Decimal):
        acct = self.account(uid)
        acct.funds += amount

    @atomic
    def withdraw(self, uid: int, amount: Decimal):
        acct = self.account(uid)
        if acct.funds < amount:
            raise LedgerError("Insufficient funds.")
        acct.funds -= amount

    @atomic
    def transfer(self, from_uid: int, to_uid: int, amount: Decimal):
        self.withdraw(from_uid, amount)
        self.deposit(to_uid, amount)
