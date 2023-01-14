from dataclasses import dataclass
from typing import Any


@dataclass
class Request:
    value: Any


@dataclass
class Prepare:
    id: int


@dataclass
class Accepted:
    id: int
    value: Any


@dataclass
class Promise:
    id: int
    prev: Accepted | None


@dataclass
class Accept:
    id: int
    value: Any


PaxosMsg = Request | Prepare | Promise | Accept | Accepted
