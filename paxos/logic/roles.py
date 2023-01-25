from collections import defaultdict
from dataclasses import dataclass
from threading import Event
from typing import Any

from paxos.logic.communication import Communicator, NodeID, PaxosMsg, Role, RoleBehavior
from paxos.logic.data import (
    Accept,
    Accepted,
    Consensus,
    Nack,
    Prepare,
    Promise,
    Query,
    QueryResponse,
    Request,
)
from paxos.logic.generator import IDGenerator


class MessageStore:
    def __init__(self, quorum_size: int):
        self.quorum_size = quorum_size
        self.consensus_id: int | None = None
        self._values: dict[int, dict[NodeID, Any]] = defaultdict(dict)

    def add(self, node_id: NodeID, val_id: int, value: Any) -> None:
        self._values[val_id][node_id] = value
        if len(self._values[val_id]) >= self.quorum_size:
            self.consensus_id = val_id

    @property
    def quorum_gathered(self) -> bool:
        return self.consensus_id is not None

    @property
    def values(self) -> list[Any]:
        if self.consensus_id is None:
            raise ValueError("Should not be called when quorum is not gathered yet")
        else:
            return list(self._values[self.consensus_id].values())

    @property
    def state(self) -> tuple[Any, Any]:
        return self.consensus_id, self._values

    @state.setter
    def state(self, value: tuple[Any, Any]):
        self.consensus_id, self._values = value


@dataclass
class Proposal:
    id: int
    value: Any


class Proposer(RoleBehavior):
    def __init__(self, comm: Communicator, id_generator: IDGenerator, quorum_size: int):
        self.comm = comm
        self.quorum_size = quorum_size
        self.id_generator = id_generator
        self.proposal: Proposal | None = None
        self.request_sent_ev = Event()
        self.consensus_reached = False
        self.promises_store = MessageStore(quorum_size)

    def request(self, value: Any):
        assert value is not None
        if self.consensus_reached:
            self.request_sent_ev.set()
            return

        req = Request(value)
        id = self.id_generator.next_id()
        self.proposal = Proposal(id, req.value)
        self.request_sent_ev.clear()
        self.comm.send(Prepare(id), self.comm.acceptors)

    def recv_nack(self, nack: Nack):
        if self.proposal is None or nack.id != self.proposal.id:
            return

        self.proposal = None
        self.request_sent_ev.set()

    def recv_promise(self, acceptor: NodeID, promise: Promise):
        if self.proposal is None or promise.id != self.proposal.id:
            return

        self.promises_store.add(acceptor, promise.id, promise)

        if self.promises_store.quorum_gathered:
            prev_accepted = [
                v.prev for v in self.promises_store.values if v.prev is not None
            ]
            if len(prev_accepted) > 0:
                accepted_value = max(
                    prev_accepted,
                    key=lambda accepted: accepted.id,
                ).value
            else:
                accepted_value = self.proposal.value

            msg = Accept(self.proposal.id, accepted_value)
            self.comm.send(msg, self.comm.acceptors)

    def recv_consensus_reached(self, msg: Consensus):
        self.consensus_reached = True

    def on_recv(self, sender: NodeID, message: PaxosMsg):
        if isinstance(message, Promise):
            self.recv_promise(sender, message)
        elif isinstance(message, Nack):
            self.recv_nack(message)
        elif isinstance(message, Consensus):
            self.recv_consensus_reached(message)

    @property
    def state(self) -> tuple[Any, Any, Any]:
        return self.proposal, self.id_generator.state, self.promises_store.state

    @state.setter
    def state(self, value: tuple[Any, Any, Any]):
        self.proposal, self.id_generator.state, self.promises_store.state = value


class Acceptor(RoleBehavior):
    def __init__(self, comm: Communicator):
        self.comm = comm
        self.promised_id: int | None = None
        self.accepted: Accepted | None = None

    def recv_prepare(self, proposer: NodeID, prepare: Prepare):
        if self.promised_id is not None and prepare.id < self.promised_id:
            self.comm.send(Nack(prepare.id), [proposer])
            return

        self.promised_id = prepare.id
        promise = Promise(self.promised_id, self.accepted)
        self.comm.send(promise, [proposer])

    def recv_accept(self, proposer: NodeID, accept: Accept):
        if self.promised_id is not None and accept.id < self.promised_id:
            return

        self.promised_id = accept.id
        self.accepted = Accepted(accept.id, accept.value)
        self.comm.send(self.accepted, self.comm.learners)

    def on_recv(self, sender: NodeID, message: PaxosMsg):
        if isinstance(message, Prepare):
            self.recv_prepare(sender, message)
        elif isinstance(message, Accept):
            self.recv_accept(sender, message)

    @property
    def state(self):
        return self.promised_id, self.accepted

    @state.setter
    def state(self, value):
        self.promised_id, self.accepted = value


class Learner(RoleBehavior):
    def __init__(self, comm: Communicator, quorum_size: int):
        self.comm = comm
        self.quorum_size = quorum_size
        self.consensus_value = None
        self.accepted_store = MessageStore(quorum_size)
        self.responses_store = MessageStore(quorum_size)
        self.response_await_ev = Event()

    def query(self):
        self.response_await_ev.clear()
        self._responses_recv = 0
        self.comm.send(Query(), self.comm.learners)

    def recv_query_resp(self, learner: NodeID, query_resp: QueryResponse):
        self.responses_store.add(learner, 0, query_resp)  # only one type of response
        if query_resp.value is None and not self.responses_store.quorum_gathered:
            return

        self._active = False
        self.consensus_value = query_resp.value
        self.response_await_ev.set()

    def recv_accepted(self, acceptor: NodeID, accepted: Accepted):
        if self.consensus_value is not None:
            return

        self.accepted_store.add(acceptor, accepted.id, accepted)
        if self.accepted_store.quorum_gathered:
            self.consensus_value = self.accepted_store.values[0].value
            self.comm.send(Consensus(self.consensus_value), self.comm.proposers)

    def recv_query(self, questioner: NodeID, query: Query):
        self.comm.send(QueryResponse(self.consensus_value), [questioner])

    def on_recv(self, sender: NodeID, message: PaxosMsg):
        if isinstance(message, Accepted):
            self.recv_accepted(sender, message)
        elif isinstance(message, Query):
            self.recv_query(sender, message)

    @property
    def state(self):
        return self.consensus_value, self.accepted_store.state

    @state.setter
    def state(self, value):
        self.consensus_value, self.accepted_store.state = value


class Server(RoleBehavior):
    """A "Paxos server", i.e. a node with all the behaviors."""

    def __init__(self, comm: Communicator, id_generator: IDGenerator):
        self.comm = comm
        quorum_size = len(self.comm.all_of(Role.ACCEPTOR)) // 2 + 1
        self.acceptor = Acceptor(self.comm)
        self.proposer = Proposer(self.comm, id_generator, quorum_size)
        self.learner = Learner(self.comm, quorum_size)

    @property
    def state(self):
        return {
            Role.ACCEPTOR: self.acceptor.state,
            Role.PROPOSER: self.proposer.state,
            Role.LEARNER: self.learner.state,
        }

    @state.setter
    def state(self, value):
        self.acceptor.state = value[Role.ACCEPTOR]
        self.proposer.state = value[Role.PROPOSER]
        self.learner.state = value[Role.LEARNER]

    def on_recv(self, sender: NodeID, message: PaxosMsg) -> None:
        for aspect in (self.proposer, self.acceptor, self.learner):
            aspect.on_recv(sender, message)
