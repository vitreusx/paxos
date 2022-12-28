from typing import Literal

from requests import post

from paxos.logic.abstract import Messenger
from paxos.logic.data import AcceptMsg, AcceptRequestMsg, Msg, PrepareMsg, PromiseMsg


class HttpMessenger(Messenger):
    def __init__(
        self,
        uid: int,
        proposers: dict[int, str],
        acceptors: dict[int, str],
        learners: dict[int, str],
    ):
        self.uid = uid
        self.proposers = proposers
        self.acceptors = acceptors
        self.learners = learners

    def _send(
        self,
        message: Msg,
        from_uid: int,
        address: str,
        role: Literal["proposer", "acceptor", "learner"],
    ):
        if isinstance(message, AcceptMsg):
            url = f"{address}/paxos/{role}/accept/{from_uid}"
        elif isinstance(message, AcceptRequestMsg):
            url = f"{address}/paxos/{role}/accept_request/{from_uid}"
        elif isinstance(message, PrepareMsg):
            url = f"{address}/paxos/{role}/prepare/{from_uid}"
        elif isinstance(message, PromiseMsg):
            url = f"{address}/paxos/{role}/promise/{from_uid}"
        else:
            raise ValueError(f"message {message} of invalid type")

        payload = message.to_json()

        post(url=url, json=payload)

    def deactivate(self, uid: int):
        self.proposers.pop(uid)
        self.acceptors.pop(uid)
        self.learners.pop(uid)

    def add(self, uid: int, address: str, roles: list[str]):
        if "proposer" in roles:
            self.proposers[uid] = address
        if "acceptor" in roles:
            self.acceptors[uid] = address
        if "learner" in roles:
            self.learners[uid] = address

    def send_prepare(self, prepare: PrepareMsg):
        """send prepare to all acceptors"""
        for uid, addr in self.acceptors.items():
            self._send(prepare, uid, addr, "acceptor")

    def send_promise(self, promise: PromiseMsg, proposer_uid: int):
        """send promise to specified proposer"""
        if proposer_uid not in self.proposers.keys():
            raise ValueError(f"{proposer_uid} unknown")
        addr = self.proposers[proposer_uid]
        self._send(promise, proposer_uid, addr, "proposer")

    def send_accept_request(self, accept_request: AcceptRequestMsg):
        """send accept request to all acceptors"""
        for uid, addr in self.acceptors.items():
            self._send(accept_request, uid, addr, "acceptor")

    def send_accept(self, accept: AcceptMsg, proposer_uid: int):
        """send accept to proposer and all learners"""
        if proposer_uid not in self.proposers.keys():
            raise ValueError(f"{proposer_uid} unknown")

        if proposer_uid not in self.learners.keys():
            addr = self.proposers[proposer_uid]
            self._send(accept, proposer_uid, addr, "proposer")

        for uid, addr in self.learners.items():
            self._send(accept, uid, addr, "learner")

    def send_consensus_reached(self, value: str):
        """inform everyone about the consensus value"""
