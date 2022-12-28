from dataclasses import dataclass

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class PrepareMsg:
    id: int


@dataclass_json
@dataclass
class PromiseMsg:
    id: int
    accepted_id: int | None = None
    accepted_value: str | None = None


@dataclass_json
@dataclass
class AcceptRequestMsg:
    id: int
    value: str


@dataclass_json
@dataclass
class AcceptMsg:
    id: int
    value: str


@dataclass
class Proposal:
    id: int | None
    value: str | None


Msg = PrepareMsg | PromiseMsg | AcceptRequestMsg | AcceptMsg
