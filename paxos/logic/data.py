from dataclasses import dataclass


@dataclass
class PrepareMsg:
    id: int


@dataclass
class PromiseMsg:
    id: int
    accepted_id: int | None
    accepted_value: str | None


@dataclass
class AcceptRequestMsg:
    id: int
    value: str


@dataclass
class AcceptMsg:
    id: int
    value: str


@dataclass
class Proposal:
    id: int
    value: str
