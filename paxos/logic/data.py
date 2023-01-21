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
class Nack:
    id: int


@dataclass
class Accept:
    id: int
    value: Any


@dataclass
class Query:
    pass


@dataclass
class QueryResponse:
    prev: Accepted | None


PaxosMsg = (
    Request | Prepare | Promise | Accept | Accepted | Nack | Query | QueryResponse
)

NodeID = int
Address = str


@dataclass
class Payload:
    """Multi-Paxos payload."""

    sender: NodeID
    key: Any
    message: PaxosMsg
