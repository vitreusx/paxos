from .ledger import Ledger, Decimal, LedgerError

from typing import Dict, Union
from pathlib import Path
from dataclasses import asdict, dataclass
import io
from dacite.config import Config
from dacite.core import from_dict
from ruamel.yaml import YAML

from paxos.utils.atomic import AtomicMixin, atomic, atomic_save


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
        with io.StringIO() as str_f:
            yaml.dump(data, str_f)
            data_txt = str_f.getvalue()
        atomic_save(data_txt, self.fpath)
