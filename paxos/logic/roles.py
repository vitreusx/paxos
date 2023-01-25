from dataclasses import dataclass, field
from threading import Event
from typing import Any
import logging

from paxos.logic.communication import Communicator, NodeID, PaxosMsg, Role, RoleBehavior
from paxos.logic.data import (
    Accept,
    Accepted,
    Nack,
    Prepare,
    Promise,
    Query,
    QueryResponse,
    Request,
)
from paxos.logic.generator import IDGenerator


logger = logging.getLogger("debug")
logger.setLevel(logging.DEBUG)


class AcceptorValues:
    def __init__(self, quorum_size: int):
        self.quorum_size = quorum_size
        self.reset()

    def reset(self):
        self.consensus_id = None
        self._values: dict[int, dict[NodeID, Any]] = {}

    def add(self, acc_id: NodeID, val_id: int, value: Any):
        if val_id not in self._values:
            self._values[val_id] = {}

        self._values[val_id][acc_id] = value
        if len(self._values[val_id]) >= self.quorum_size:
            self.consensus_id = val_id

    @property
    def values(self):
        if self.consensus_id is None:
            return None
        else:
            return [*self._values[self.consensus_id].values()]


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
        self.promises = AcceptorValues(self.quorum_size)

    def request(self, value: Any):
        assert value is not None
        req = Request(value)
        id = self.id_generator.next_id()
        self.proposal = Proposal(id=id, value=req.value)
        self.promises.reset()
        self.comm.send(Prepare(id), self.comm.acceptors)

    def recv_nack(self, nack: Nack):
        if self.proposal is None or nack.id != self.proposal.id:
            return
        self.proposal = None

    def recv_promise(self, acceptor: NodeID, promise: Promise):
        if self.proposal is None or promise.id != self.proposal.id:
            return

        self.promises.add(acceptor, promise.id, promise)
        if self.promises.values is not None:
            promises: list[Promise] = self.promises.values

            accepted_value, accepted_id = self.proposal.value, None
            for promise in promises:
                if promise.prev is None:
                    continue
                if accepted_id is None or promise.prev.id > accepted_id:
                    accepted_id = promise.prev.id
                    accepted_value = promise.prev.value

            msg = Accept(self.proposal.id, accepted_value)
            self.comm.send(msg, self.comm.acceptors)

    def on_recv(self, sender: NodeID, message: PaxosMsg):
        if isinstance(message, Promise):
            self.recv_promise(sender, message)
        elif isinstance(message, Nack):
            self.recv_nack(message)

    @property
    def state(self):
        return self.proposal, self.id_generator.state, self.promises

    @state.setter
    def state(self, value):
        self.proposal, self.id_generator.state, self.promises = value


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
        if self.promised_id is None or accept.id < self.promised_id:
            return

        self.promised_id = accept.id
        self.accepted = Accepted(accept.id, accept.value)
        self.comm.send(self.accepted, {proposer, *self.comm.learners})

    def recv_query(self, learner: NodeID, query: Query):
        self.comm.send(QueryResponse(self.accepted), [learner])

    def on_recv(self, sender: NodeID, message: PaxosMsg):
        if isinstance(message, Prepare):
            self.recv_prepare(sender, message)
        elif isinstance(message, Accept):
            self.recv_accept(sender, message)
        elif isinstance(message, Query):
            self.recv_query(sender, message)

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
        self.value = None
        self.value_found_ev = Event()
        self.accepted = AcceptorValues(self.quorum_size)

    def query(self):
        if self.value is not None:
            self.value_found_ev.set()
        else:
            self.value_found_ev.clear()
            self.accepted.reset()
            self.comm.send(Query(), self.comm.acceptors)

    def recv_query_response(self, acceptor: NodeID, resp: QueryResponse):
        if self.value is not None:
            return

        if resp.value is not None:
            acc = resp.value
            self.accepted.add(acceptor, acc.id, acc.value)
        else:
            self.accepted.add(acceptor, -1, None)

        if self.accepted.values is not None:
            self.value = self.accepted.values[0]
            self.value_found_ev.set()

    def recv_accepted(self, acceptor: NodeID, acc: Accepted):
        if self.value is not None:
            return

        self.accepted.add(acceptor, acc.id, acc.value)
        if self.accepted.values is not None:
            self.value = self.accepted.values[0]
            self.value_found_ev.set()

    def on_recv(self, sender: NodeID, message: PaxosMsg) -> None:
        if isinstance(message, QueryResponse):
            self.recv_query_response(sender, message)
        elif isinstance(message, Accepted):
            self.recv_accepted(sender, message)

    @property
    def state(self):
        return self.value, self.accepted

    @state.setter
    def state(self, value):
        self.value, self.accepted = value


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
