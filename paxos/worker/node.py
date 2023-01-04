from typing import Literal

from paxos.logic.abstract import IDGenerator
from paxos.logic.core import Acceptor, Learner, Proposer
from paxos.logic.data import AcceptMsg, AcceptRequestMsg, PrepareMsg, PromiseMsg
from paxos.worker.messenger import HttpMessenger


class Node:
    def __init__(
        self,
        uid: int,
        quorum_size: int,
        id_generator: IDGenerator,
        messenger: HttpMessenger,
        addr_map: dict[int, str],
    ):
        self.uid = uid
        self.messenger = messenger
        self.addr_map = addr_map

        self.proposer = Proposer(uid, quorum_size, id_generator, self.messenger)
        self.acceptor = Acceptor(uid, quorum_size, self.messenger)
        self.learner = Learner(uid, quorum_size, self.messenger)

        self.refresh_messenger()

    def refresh_messenger(self):
        # TODO: include healthchecks, revive and forget about nodes
        for uid, addr in self.addr_map.items():
            self.messenger.add(uid, addr, roles=["proposer", "acceptor", "learner"])

    def handle_paxos_msg(
        self,
        role: Literal["proposer", "acceptor", "learner"],
        msg_type: Literal["prepare", "promise", "accept_request", "accept"],
        req_json: str,
        from_uid: int,
    ):
        if role not in ("proposer", "acceptor", "learner"):
            raise ValueError(f"unknown role {role}")

        if msg_type == "prepare":
            message = PrepareMsg.from_json(req_json)
        elif msg_type == "promise":
            message = PromiseMsg.from_json(req_json)
        elif msg_type == "accept_request":
            message = AcceptRequestMsg.from_json(req_json)
        elif msg_type == "accept":
            message = AcceptMsg.from_json(req_json)
        else:
            raise ValueError(f"unknown msg_type {msg_type}")

        match role, msg_type:
            case "proposer", "promise":
                self.proposer.recv_promise(message, from_uid)
            case "proposer", "accept":
                self.proposer.recv_accept(message, from_uid)
            case "proposer", "prepare":
                self.acceptor.recv_prepare(message, from_uid)
            case "acceptor", "accept_request":
                self.acceptor.recv_accept_request(message, from_uid)
            case "learner", "accept":
                self.learner.recv_accept(message, from_uid)
            case _:
                raise ValueError(
                    f"unknown combination role={role}, msg_type={msg_type}"
                )
