from time import sleep
from typing import Dict

from paxos.logic.abstract import Messenger
from paxos.logic.core import Acceptor, Learner, Proposer
from paxos.logic.data import AcceptMsg, AcceptRequestMsg, PrepareMsg, PromiseMsg


class HttpMessenger(Messenger):
    def __init__(self, uid: int):
        self.uid = str(uid)

    def init_nodes(
        self,
        proposers: Dict[str, Proposer],
        acceptors: Dict[str, Acceptor],
        learners: Dict[str, Learner],
    ):
        self.proposers = proposers
        self.acceptors = acceptors
        self.learners = learners

    def send_prepare(self, prepare: PrepareMsg):
        """send prepare to all acceptors"""
        for uid, acceptor in self.acceptors.items():
            print(f"from {self.uid} to acceptor {uid}: {prepare}")
            acceptor.recv_prepare(prepare, self.uid)
            # sleep(0.5)

    def send_promise(self, promise: PromiseMsg, proposer_uid: str):
        """send promise to specified proposer"""
        proposer = self.proposers.get(proposer_uid)
        print(f"from {self.uid} to proposer {proposer_uid}: {promise}")
        if proposer is None:
            print(f"error: proposer {proposer_uid} unknown")
            print(f"{self.proposers=}")
            raise ValueError
        proposer.recv_promise(promise, self.uid)
        # sleep(0.5)

    def send_accept_request(self, accept_request: AcceptRequestMsg):
        """send accept request to all acceptors"""
        for uid, acceptor in self.acceptors.items():
            print(f"from {self.uid} to acceptor {uid}: {accept_request}")
            acceptor.recv_accept_request(accept_request, self.uid)
            # sleep(0.5)

    def send_accept(self, accept: AcceptMsg, proposer_uid: str):
        """send accept to proposer and all learners"""
        if proposer_uid not in self.learners.keys():
            proposer = self.proposers.get(proposer_uid)
            print(f"from {self.uid} to proposer {proposer_uid}: {accept}")
            if proposer is None:
                print(f"error: proposer {proposer_uid} unknown")
                print(f"{self.proposers=}")
                raise ValueError

            proposer.recv_accept(accept, self.uid)
            # sleep(0.5)

        for uid, learner in self.learners.items():
            print(f"from {self.uid} to learner {uid}: {accept}")
            learner.recv_accept(accept, self.uid)
            # sleep(0.5)

    def send_consensus_reached(self, value: str):
        """inform everyone about the consensus value"""
        print(f"{self.uid} says that consensus has been reached on value {value}")
