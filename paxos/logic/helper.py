from collections import defaultdict
from typing import Dict, Set

from paxos.logic.abstract import IDGenerator, Messenger
from paxos.logic.data import AcceptMsg, AcceptRequestMsg, PrepareMsg, PromiseMsg


class BasicIDGenerator(IDGenerator):
    def __init__(self, node_id: int, num_overall: int):
        self.node_id = node_id
        self.num_overall = num_overall
        self.last_id = node_id

    def new_id(self) -> int:
        self.last_id += self.num_overall
        return self.last_id


class BasicMessenger(Messenger):
    def __init__(self, proposers: Set[str], acceptors: Set[str], learners: Set[str]):
        self.proposers = proposers
        self.acceptors = acceptors
        self.learners = learners

    def send_prepare(self, prepare: PrepareMsg):
        """send prepare to all acceptors"""

    def send_promise(self, promise: PromiseMsg, to_proposer: str):
        """send promise to specified proposer"""

    def send_accept_request(self, accept_request: AcceptRequestMsg):
        """send accept request to all acceptors"""

    def send_accept(self, accept: AcceptMsg, to_proposer: str):
        """send accept to proposer and all learners"""

    def send_consensus_reached(self, value: str):
        """inform everyone about the consensus value"""


class AcceptStore:
    def __init__(self, quorum_size: int):
        self.quorum_size = quorum_size

        self.acceptor_to_proposal_id: Dict[str, int] = {}
        self.proposal_id_to_acceptors: Dict[int, Set[str]] = defaultdict(set)
        self.consensus_value: str | None = None

    def get_last_proposal_id(self, from_id: int) -> int | None:
        return self.acceptor_to_proposal_id.get(from_id)

    def add_new_proposal(self, accept: AcceptMsg, from_id: str):
        last_proposal_id = self.get_last_proposal_id(from_id)
        # remove previous declaration
        if last_proposal_id is not None:
            self.proposal_id_to_acceptors[last_proposal_id].remove(from_id)
            if len(self.proposal_id_to_acceptors[last_proposal_id]) == 0:
                self.proposal_id_to_acceptors.pop(last_proposal_id)

        self.acceptor_to_proposal_id[from_id] = accept.id
        self.proposal_id_to_acceptors[accept.id].add(from_id)

        # check for consensus
        if len(self.proposal_id_to_acceptors) >= self.quorum_size:
            self.consensus_value = accept.value

    def get_consensus_value(self) -> str | None:
        return self.consensus_value

    def reset(self):
        self.acceptor_to_proposal_id = {}
        self.proposal_id_to_acceptors = defaultdict(set)
        self.consensus_value = None
