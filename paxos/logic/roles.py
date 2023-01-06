from dataclasses import dataclass, field
from .comm import *
from .data import *


@dataclass
class Proposal:
    id: int
    value: Any
    acceptor_ids: set[int] = field(default_factory=lambda: set())
    prev_accepted: list[Accepted] = field(default_factory=lambda: [])


class Proposer(RoleBehavior):
    def __init__(self, comm: Communicator, quorum_size: int):
        self.comm = comm
        self.quorum_size = quorum_size
        self._next_id = 0
        self.proposal: Proposal | None = None

    def request(self, value: Any):
        self.recv_request(Request(value))

    def recv_request(self, req: Request):
        id = self._next_id
        self.proposal = Proposal(id, req.value)
        self._next_id += 1
        self.comm.send(Prepare(id), self.comm.acceptors)

    def recv_promise(self, acceptor: int, promise: Promise):
        if self.proposal is None:
            return

        if acceptor in self.proposal.acceptor_ids:
            return
        if promise.id < self.proposal.id:
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
                self.proposal.value = accepted_value

            msg = Accept(self.proposal.id, self.proposal.value)
            self.comm.send(msg, self.proposal.acceptor_ids)
            # self.comm.send(msg, self.comm.acceptors)

    def recv_accepted(self, accepted: Accepted):
        pass

    def on_recv(self, sender: int, message: PaxosMsg):
        if isinstance(message, Request):
            self.recv_request(message)
        elif isinstance(message, Promise):
            self.recv_promise(sender, message)
        elif isinstance(message, Accepted):
            self.recv_accepted(message)

    @property
    def state(self):
        return self.proposal

    @state.setter
    def state(self, value):
        self.proposal = value


class Acceptor(RoleBehavior):
    def __init__(self, comm: Communicator):
        self.comm = comm
        self.promised_id: int | None = None
        self.accepted: Accepted | None = None

    def recv_prepare(self, proposer: int, prepare: Prepare):
        if self.promised_id is not None and prepare.id < self.promised_id:
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

    def on_recv(self, sender: int, message: PaxosMsg):
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
    def __init__(self, comm: Communicator):
        self.comm = comm
        self.value = None
        self.has_value = False

    def recv_accepted(self, accepted: Accepted):
        self.value = accepted.value
        self.has_value = True

    def on_recv(self, sender: int, message: PaxosMsg):
        if isinstance(message, Accepted):
            self.recv_accepted(message)

    @property
    def state(self):
        return self.value, self.has_value

    @state.setter
    def state(self, value):
        self.value, self.has_value = value
