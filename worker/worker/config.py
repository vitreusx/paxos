from __future__ import annotations
from dataclasses import dataclass
import os


@dataclass
class Config:
    LEDGER_PATH: str

    @staticmethod
    def from_env() -> Config:
        conf_values = {}
        for field in Config.__dataclass_fields__:
            value = os.getenv(field)
            if value is not None:
                conf_values[field] = value

        return Config(**conf_values)
