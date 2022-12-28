from collections import defaultdict
from typing import Dict, Set

from paxos.logic.abstract import IDGenerator
from paxos.logic.data import AcceptMsg


class BasicIDGenerator(IDGenerator):
    def __init__(self, node_id: int, num_overall: int):
        self.node_id = node_id
        self.num_overall = num_overall
        self.last_id = node_id

    def new_id(self) -> int:
        self.last_id += self.num_overall
        return self.last_id


class AcceptStore:
    def __init__(self, quorum_size: int):
        self.quorum_size = quorum_size

        self.acceptor_to_proposal_id: Dict[str, int] = {}
        self.proposal_id_to_acceptors: Dict[int, Set[str]] = defaultdict(set)
        self.consensus_value: str | None = None

    def __repr__(self) -> str:
        return f"AcceptStore(prop_id -> acceptors: {dict(self.proposal_id_to_acceptors)}, consensus_val={self.consensus_value})"

    def get_last_proposal_id(self, from_uid: int) -> int | None:
        return self.acceptor_to_proposal_id.get(from_uid)

    def add_new_proposal(self, accept: AcceptMsg, from_uid: str):
        last_proposal_id = self.get_last_proposal_id(from_uid)
        # remove previous declaration
        if last_proposal_id is not None:
            self.proposal_id_to_acceptors[last_proposal_id].remove(from_uid)
            if len(self.proposal_id_to_acceptors[last_proposal_id]) == 0:
                self.proposal_id_to_acceptors.pop(last_proposal_id)

        self.acceptor_to_proposal_id[from_uid] = accept.id
        self.proposal_id_to_acceptors[accept.id].add(from_uid)

        # check for consensus
        if len(self.proposal_id_to_acceptors[accept.id]) >= self.quorum_size:
            self.consensus_value = accept.value

    def get_consensus_value(self) -> str | None:
        return self.consensus_value

    def reset(self):
        self.acceptor_to_proposal_id = {}
        self.proposal_id_to_acceptors = defaultdict(set)
        self.consensus_value = None
