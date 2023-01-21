from dataclasses import dataclass, field
from threading import Event
from typing import Any

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


@dataclass
class Proposal:
    id: int
    value: Any
    acceptor_ids: set[int] = field(default_factory=set)
    prev_accepted: list[Accepted] = field(default_factory=list)


class Proposer(RoleBehavior):
    def __init__(self, comm: Communicator, id_generator: IDGenerator, quorum_size: int):
        self.comm = comm
        self.quorum_size = quorum_size
        self.id_generator = id_generator
        self.proposal: Proposal | None = None
        self.value = None
        self.value_set_ev = Event()

    def request(self, value: Any):
        assert value is not None
        req = Request(value)
        id = self.id_generator.next_id()
        self.proposal = Proposal(id, req.value)
        self.value_set_ev.clear()
        self.comm.send(Prepare(id), self.comm.acceptors)

    def recv_nack(self, nack: Nack):
        if self.proposal is None or nack.id != self.proposal.id:
            return

        self.proposal = self.value = None
        self.value_set_ev.set()

    def recv_promise(self, acceptor: int, promise: Promise):
        if self.proposal is None or promise.id != self.proposal.id:
            return

        if acceptor in self.proposal.acceptor_ids:
            return

        self.proposal.acceptor_ids.add(acceptor)

        if promise.prev is not None:
            self.proposal.prev_accepted.append(promise.prev)

        if len(self.proposal.acceptor_ids) >= self.quorum_size:
            accepted_value = self.proposal.value
            if len(self.proposal.prev_accepted) > 0:
                accepted_value = max(
                    self.proposal.prev_accepted,
                    key=lambda accepted: accepted.id,
                ).value

            msg = Accept(self.proposal.id, accepted_value)
            self.comm.send(msg, self.proposal.acceptor_ids)

    def recv_accepted(self, accepted: Accepted):
        if self.proposal is None:
            return

        self.proposal = None
        self.value = accepted.value
        self.value_set_ev.set()

    def on_recv(self, sender: int, message: PaxosMsg):
        if isinstance(message, Promise):
            self.recv_promise(sender, message)
        elif isinstance(message, Accepted):
            self.recv_accepted(message)
        elif isinstance(message, Nack):
            self.recv_nack(message)

    @property
    def state(self) -> tuple[Any, Any]:
        return self.value, self.id_generator.state

    @state.setter
    def state(self, value: tuple[Any, Any]):
        self.value, self.id_generator.state = value


class Questioner(RoleBehavior):
    def __init__(self, comm: Communicator, quorum_size: int):
        self.comm = comm
        self.quorum_size = quorum_size
        self.acceptor_ids: set[NodeID] = set()
        self.prev_accepted: list[Accepted] = []
        self.value = None
        self.value_set_ev = Event()
        self._active = False

    def query(self):
        self.value_set_ev.clear()
        self.acceptor_ids = set()
        self.prev_accepted = []
        self._active = True
        self.comm.send(Query(), self.comm.acceptors)

    def recv_query_resp(self, acceptor: int, query_resp: QueryResponse):
        if not self._active:
            return

        if acceptor in self.acceptor_ids:
            return

        self.acceptor_ids.add(acceptor)
        if query_resp.prev is not None:
            self.prev_accepted.append(query_resp.prev)

        if len(self.acceptor_ids) >= self.quorum_size:
            accepted_value = None
            if len(self.prev_accepted) > 0:
                accepted_value = max(
                    self.prev_accepted,
                    key=lambda accepted: accepted.id,
                ).value

            self._active = False
            self.value = accepted_value
            self.value_set_ev.set()

    def on_recv(self, sender: NodeID, message: PaxosMsg) -> None:
        if isinstance(message, QueryResponse):
            self.recv_query_resp(sender, message)

    @property
    def state(self):
        return self.value

    @state.setter
    def state(self, value):
        self.value = value


class Acceptor(RoleBehavior):
    def __init__(self, comm: Communicator):
        self.comm = comm
        self.promised_id: int | None = None
        self.accepted: Accepted | None = None

    def recv_prepare(self, proposer: int, prepare: Prepare):
        if self.promised_id is not None and prepare.id < self.promised_id:
            self.comm.send(Nack(prepare.id), [proposer])
            return

        self.promised_id = prepare.id
        promise = Promise(self.promised_id, self.accepted)
        self.comm.send(promise, [proposer])

    def recv_accept(self, proposer: int, accept: Accept):
        if self.promised_id is not None and accept.id < self.promised_id:
            return

        self.promised_id = accept.id
        self.accepted = Accepted(accept.id, accept.value)
        self.comm.send(self.accepted, [proposer, *self.comm.learners])

    def recv_query(self, questioner: int, query: Query):
        resp = QueryResponse(prev=self.accepted)
        self.comm.send(resp, [questioner])

    def on_recv(self, sender: int, message: PaxosMsg):
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
    def __init__(self, comm: Communicator):
        self.comm = comm
        self.value = None

    def recv_accepted(self, accepted: Accepted):
        self.value = accepted.value

    def on_recv(self, sender: int, message: PaxosMsg):
        if isinstance(message, Accepted):
            self.recv_accepted(message)

    @property
    def state(self):
        return self.value

    @state.setter
    def state(self, value):
        self.value = value


class Server(RoleBehavior):
    """A "Paxos server", i.e. a node with all the behaviors."""

    def __init__(self, comm: Communicator, id_generator: IDGenerator):
        self.comm = comm
        self.acceptor = Acceptor(self.comm)
        quorum_size = len(self.comm.all_of(Role.ACCEPTOR)) // 2 + 1
        self.proposer = Proposer(self.comm, id_generator, quorum_size)
        self.questioner = Questioner(self.comm, quorum_size)
        self.learner = Learner(self.comm)

    @property
    def state(self):
        return {
            Role.ACCEPTOR: self.acceptor.state,
            Role.PROPOSER: self.proposer.state,
            Role.LEARNER: self.learner.state,
            Role.QUESTIONER: self.questioner.state,
        }

    @state.setter
    def state(self, value):
        self.acceptor.state = value[Role.ACCEPTOR]
        self.proposer.state = value[Role.PROPOSER]
        self.learner.state = value[Role.LEARNER]
        self.questioner.state = value[Role.QUESTIONER]

    def on_recv(self, sender: NodeID, message: PaxosMsg) -> None:
        for aspect in (self.proposer, self.acceptor, self.learner, self.questioner):
            aspect.on_recv(sender, message)
