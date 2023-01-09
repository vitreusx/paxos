import os
import shutil
import tempfile
from copy import deepcopy
from functools import wraps
from pathlib import Path
from typing import Union


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


def atomic_save(data: Union[str, bytes], dst: Path):
    mode = "w" if isinstance(data, str) else "wb"
    with tempfile.NamedTemporaryFile(mode=mode, delete=False) as tmpfile:
        tmpfile.write(data)
        tmpfile.flush()
        os.fsync(tmpfile.fileno())
        shutil.move(tmpfile.name, dst)
