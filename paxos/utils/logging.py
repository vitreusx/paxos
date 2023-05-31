from typing import Any
from uuid import UUID

from paxos.logic import data
from paxos.logic.ledger.base import Deposit, OpenAccount, Transfer, Withdraw
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
        body = f"{format_value(paxos_msg.value)}"
    elif isinstance(paxos_msg, data.Consensus):
        body = format_value(paxos_msg.value)
    else:
        body = paxos_msg

    return f" {sender_id} --> {receiver_id} | key = {key} | {type(paxos_msg).__name__.upper()} {body}"


def format_accept_body(msg: data.Accept | data.Accepted | None) -> str:
    if msg is None:
        return str(msg)
    return f"{msg.id} {format_value(msg.value)}"


def format_value(value: Any) -> str:
    if isinstance(value, PaxosVar.SetValue):
        body = str(value.new_value)
    elif isinstance(value, OpenAccount):
        body = "OPENACCOUNT"
    elif isinstance(value, Deposit):
        body = f"DEPOSIT {value.amount}$ to {value.uid}"
    elif isinstance(value, Withdraw):
        body = f"WITHDRAW {value.amount}$ from {value.uid}"
    elif isinstance(value, Transfer):
        body = f"TRANSFER {value.amount}$ {value.from_uid} -> {value.to_uid}"
    else:
        body = str(value)
    return f"[{body}]"
