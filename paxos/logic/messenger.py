from typing import List, Literal, Set

from paxos.logic.abstract import Messenger
from paxos.logic.data import AcceptMsg, AcceptRequestMsg, Msg, PrepareMsg, PromiseMsg


class HttpMessenger(Messenger):
    def __init__(
        self,
        uid: str,
        proposers: Set[str],
        acceptors: Set[str],
        learners: Set[str],
    ):
        self.uid = uid
        self.proposers = proposers
        self.acceptors = acceptors
        self.learners = learners

    def _send(
        self, message: Msg, host: str, role: Literal["proposer", "acceptor", "learner"]
    ):
        if isinstance(message, AcceptMsg):
            address = f"{host}/{role}/accept"
        elif isinstance(message, AcceptRequestMsg):
            address = f"{host}/{role}/accept_request"
        elif isinstance(message, PrepareMsg):
            address = f"{host}/{role}/prepare"
        elif isinstance(message, PromiseMsg):
            address = f"{host}/{role}/promise"
        else:
            raise ValueError(f"message {message} of invalid type")

        # TODO: actually send it

    def deactivate(self, address: str):
        self.proposers.pop(address)
        self.acceptors.pop(address)
        self.learners.pop(address)

    def add(self, address: str, roles: List[str]):
        if "proposer" in roles:
            self.proposers.add(address)
        if "acceptor" in roles:
            self.acceptors.add(address)
        if "learner" in roles:
            self.learner.add(address)

    def send_prepare(self, prepare: PrepareMsg):
        """send prepare to all acceptors"""
        for acceptor_addr in self.acceptors:
            self._send(prepare, acceptor_addr, "acceptor")

    def send_promise(self, promise: PromiseMsg, proposer_addr: str):
        """send promise to specified proposer"""
        if proposer_addr not in self.proposers:
            raise ValueError(f"{proposer_addr} unknown")
        self._send(promise, proposer_addr, "proposer")

    def send_accept_request(self, accept_request: AcceptRequestMsg):
        """send accept request to all acceptors"""
        for acceptor_addr in self.acceptors:
            self._send(accept_request, acceptor_addr, "acceptor")

    def send_accept(self, accept: AcceptMsg, proposer_addr: str):
        """send accept to proposer and all learners"""
        if proposer_addr not in self.proposers:
            raise ValueError(f"{proposer_addr} unknown")

        if proposer_addr not in self.learners:
            self._send(accept, proposer_addr, "proposer")

        for learner_addr in self.learners:
            self._send(accept, learner_addr, "learner")

    def send_consensus_reached(self, value: str):
        """inform everyone about the consensus value"""
