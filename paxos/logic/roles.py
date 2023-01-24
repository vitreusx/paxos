from dataclasses import dataclass, field
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


@dataclass
class Proposal:
    id: int
    value: Any
    acceptor_ids: set[NodeID] = field(default_factory=set)
    prev_accepted: list[Accepted] = field(default_factory=list)


class Proposer(RoleBehavior):
    def __init__(self, comm: Communicator, id_generator: IDGenerator, quorum_size: int):
        self.comm = comm
        self.quorum_size = quorum_size
        self.id_generator = id_generator
        self.proposal: Proposal | None = None
        self.request_sent_ev = Event()
        self.consensus_reached = False

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

        if acceptor in self.proposal.acceptor_ids:
            return

        self.proposal.acceptor_ids.add(acceptor)

        if promise.prev is not None:
            self.proposal.prev_accepted.append(promise.prev)

        if len(self.proposal.acceptor_ids) >= self.quorum_size:
            if len(self.proposal.prev_accepted) > 0:
                accepted_value = max(
                    self.proposal.prev_accepted,
                    key=lambda accepted: accepted.id,
                ).value
            else:
                accepted_value = self.proposal.value

            msg = Accept(self.proposal.id, accepted_value)
            self.comm.send(msg, self.proposal.acceptor_ids)

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
    def state(self) -> tuple[Any, Any]:
        return self.proposal, self.id_generator.state

    @state.setter
    def state(self, value: tuple[Any, Any]):
        self.proposal, self.id_generator.state = value


class Questioner(RoleBehavior):
    def __init__(self, comm: Communicator):
        self.comm = comm
        self.value = None
        self.response_await_ev = Event()
        self._active = False

    def query(self):
        self.response_await_ev.clear()
        self._active = True
        self.comm.send(Query(), self.comm.learners)

    def recv_query_resp(self, learner: NodeID, query_resp: QueryResponse):
        if not self._active or query_resp.value is None:
            return

        self._active = False
        self.value = query_resp.value
        self.response_await_ev.set()

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
        self.acceptor_to_msg: dict[NodeID, Accepted] = {}

    def recv_accepted(self, acceptor: NodeID, accepted: Accepted):
        if self.consensus_value is not None:
            return

        self.acceptor_to_msg[acceptor] = accepted
        this_id_count = sum(
            msg.id == accepted.id for msg in self.acceptor_to_msg.values()
        )
        if this_id_count >= self.quorum_size:
            self.consensus_value = accepted.value
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
        return self.consensus_value, self.acceptor_to_msg

    @state.setter
    def state(self, value):
        self.consensus_value, self.acceptor_to_msg = value


class Server(RoleBehavior):
    """A "Paxos server", i.e. a node with all the behaviors."""

    def __init__(self, comm: Communicator, id_generator: IDGenerator):
        self.comm = comm
        quorum_size = len(self.comm.all_of(Role.ACCEPTOR)) // 2 + 1
        self.acceptor = Acceptor(self.comm)
        self.proposer = Proposer(self.comm, id_generator, quorum_size)
        self.questioner = Questioner(self.comm)
        self.learner = Learner(self.comm, quorum_size)

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
