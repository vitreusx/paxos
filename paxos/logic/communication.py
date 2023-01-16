from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Iterable, List

from paxos.logic.data import PaxosMsg

NodeID = int
Address = str


class Role(Enum):
    """Role of a node in the Paxos protocol."""

    PROPOSER = 0
    ACCEPTOR = 1
    LEARNER = 2
    QUESTIONER = 3


class Communicator(ABC):
    """Abstract Paxos communicator for sending the messages."""

    @abstractmethod
    def send(self, message: PaxosMsg, to: Iterable[NodeID]):
        """Send Paxos message to a number of other nodes."""

    @abstractmethod
    def all_of(self, role: Role) -> List[NodeID]:
        """Get IDs of all nodes in the network with a given role."""

    @property
    def acceptors(self):
        return self.all_of(Role.ACCEPTOR)

    @property
    def learners(self):
        return self.all_of(Role.LEARNER)


@dataclass
class Node:
    """Data about a node in the network."""

    id: NodeID
    addr: Address
    roles: set[Role]


@dataclass
class Network:
    """Paxos over a network."""

    nodes: dict[NodeID, Node]
    me: Node

    @staticmethod
    def from_addresses(addrs: Iterable[Address], addr: Address) -> Network:
        """Construct a Paxos network from the addresses of the nodes.
        :param addrs: Addresses of all the nodes in the network.
        :param addr: Address of the calling process in the network."""

        addrs_ids = [(addr, idx) for idx, addr in enumerate(addrs)]
        addrs_ids = sorted(addrs_ids, key=lambda x: x[0])

        my_id = None
        for node_addr, node_id in addrs_ids:
            if addr == node_addr:
                my_id = node_id
                break

        assert my_id is not None

        all_roles = {Role.ACCEPTOR, Role.LEARNER, Role.PROPOSER}
        nodes = {uid: Node(uid, addr, all_roles) for addr, uid in addrs_ids}
        return Network(nodes, nodes[my_id])

    def all_of(self, role: Role) -> Iterable[Node]:
        return [node for node in self.nodes.values() if role in node.roles]

    def __getitem__(self, uid: NodeID) -> Node:
        return self.nodes[uid]

    def __len__(self) -> int:
        return len(self.nodes)


class RoleBehavior(ABC):
    """Behavior for a given role in the Paxos protocol."""

    @abstractmethod
    def on_recv(self, sender: NodeID, message: PaxosMsg) -> None:
        """Perform an action on receiving a message."""
