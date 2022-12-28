from abc import ABC, abstractmethod

from paxos.logic.data import AcceptMsg, AcceptRequestMsg, PrepareMsg, PromiseMsg


class IDGenerator(ABC):
    @abstractmethod
    def new_id(self) -> int:
        pass


class Messenger(ABC):
    @abstractmethod
    def send_prepare(self, prepare: PrepareMsg):
        """send prepare to all acceptors"""

    @abstractmethod
    def send_promise(self, promise: PromiseMsg, proposer_uid: int):
        """send promise to specified proposer"""

    @abstractmethod
    def send_accept_request(self, accept_request: AcceptRequestMsg):
        """send accept request to all acceptors"""

    @abstractmethod
    def send_accept(self, accept: AcceptMsg, proposer_uid: int):
        """send accept to proposer and all learners"""

    @abstractmethod
    def send_consensus_reached(self, value: str):
        """inform everyone about the consensus value"""
