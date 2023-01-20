from typing import Any
from uuid import UUID

from paxos.ledger.base import Deposit, LedgerCmd, OpenAccount, Transfer, Withdraw
from paxos.logic import data
from paxos.logic.types import PaxosVar


def format_payload(payload: data.Payload, receiver_id: int) -> str:
    sender_id = payload.sender
    paxos_msg = payload.message
    key = payload.key
    if isinstance(paxos_msg, data.Request):
        body = str(paxos_msg.value)
    elif isinstance(paxos_msg, (data.Prepare, data.Nack)):
        body = str(paxos_msg.id)
    elif isinstance(paxos_msg, data.Promise):
        body = str(paxos_msg.id)
        if paxos_msg.prev is not None:
            body += f", prev: {format_accept_body(paxos_msg.prev)}"
    elif isinstance(paxos_msg, (data.Accept, data.Accepted)):
        body = format_accept_body(paxos_msg)
    elif isinstance(paxos_msg, data.Query):
        body = ""
    elif isinstance(paxos_msg, data.QueryResponse):
        body = f"{format_accept_body(paxos_msg.prev)}"
    else:
        body = paxos_msg

    return f" {sender_id} --> {receiver_id} | key = {key} | {type(paxos_msg).__name__.upper()} {body}"


def format_accept_body(msg: data.Accept | data.Accepted | None) -> str:
    if msg is None:
        return str(msg)
    return f"{msg.id} [{format_value(msg.value)}]"


def format_value(value: Any) -> str:
    match value:
        case uuid, val:
            if not isinstance(uuid, UUID):
                return str(value)
            if isinstance(val, PaxosVar.SetValue):
                return str(val.new_value)
            elif isinstance(val, OpenAccount):
                return "OPENACCOUNT"
            elif isinstance(val, Deposit):
                return f"DEPOSIT {val.amount}$ to {val.uid}"
            elif isinstance(val, Withdraw):
                return f"WITHDRAW {val.amount}$ from {val.uid}"
            elif isinstance(val, Transfer):
                return f"TRANSFER {val.amount}$ {val.from_uid} -> {val.to_uid}"
    return str(value)
