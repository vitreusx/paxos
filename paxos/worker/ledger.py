from __future__ import annotations
from typing import Dict, Union
from dataclasses import dataclass, asdict
from dacite.core import from_dict
from dacite.config import Config
from ruamel.yaml import YAML
from decimal import Decimal
from pathlib import Path
from copy import deepcopy
from functools import wraps
import tempfile
import os
import shutil


class LedgerError(Exception):
    pass


class AtomicMixin:
    def __init__(self):
        self.in_tx = False
        self.prev_state = None

    def commit(self):
        pass

    def restore(self):
        pass


def atomic(method):
    @wraps(method)
    def atomic_func(self, *args, **kwargs):
        nested_tx = self.in_tx
        if not nested_tx:
            self.prev_state = deepcopy(self)
            self.in_tx = True

        try:
            retval = method(self, *args, **kwargs)
            if not nested_tx:
                self.commit()
                self.in_tx = False

            return retval
        except Exception as e:
            self.restore()
            raise e

    return atomic_func


@dataclass
class Account:
    uid: int
    funds: Decimal


@dataclass
class Ledger(AtomicMixin):
    accounts: Dict[int, Account]
    next_uid: int

    def __post_init__(self):
        AtomicMixin.__init__(self)

    def commit(self):
        pass

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


def Decimal_repr(representer, value: Decimal):
    return representer.represent_data(str(value))


def Decimal_ctor(constructor, node):
    return Decimal(node.value)


yaml = YAML(typ="unsafe")
yaml.representer.add_representer(Decimal, Decimal_repr)
yaml.constructor.add_constructor(Decimal, Decimal_ctor)


class FileLedger(Ledger):
    def __init__(self, fpath: Union[str, Path]):
        super().__init__(accounts={}, next_uid=0)

        self.fpath = Path(fpath)
        if self.fpath.exists():
            with open(self.fpath, mode="r") as f:
                data = yaml.load(f)
                state = from_dict(Ledger, data, Config(cast=[Decimal]))
                self._assign(state)

    def commit(self):
        super().commit()

        data = asdict(self)
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmpfile:
            yaml.dump(data, tmpfile)
            tmpfile.flush()
            os.fsync(tmpfile.fileno())
            shutil.move(tmpfile.name, self.fpath)
