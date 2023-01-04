from __future__ import annotations

from paxos.logic.abstract import IDGenerator, Messenger
from paxos.logic.data import (
    AcceptMsg,
    AcceptRequestMsg,
    PrepareMsg,
    PromiseMsg,
    Proposal,
)
from paxos.logic.helper import AcceptStore


class Proposer:
    def __init__(
        self,
        uid: int,
        quorum_size: int,
        id_generator: IDGenerator,
        messenger: Messenger,
    ):
        self.uid = uid
        self.quorum_size = quorum_size
        self.id_generator = id_generator
        self.messenger = messenger
        self.proposal: Proposal = Proposal(None, None)
        self.promises_rcvd: set[int] = set()

    def __repr__(self) -> str:
        return f"{type(self).__name__}(uid={self.uid})"

    def propose(self, value: str):
        self.proposal.value = value
        self.prepare()

    def prepare(self):
        id = self.id_generator.new_id()
        self.proposal.id = id
        self.messenger.send_prepare(PrepareMsg(id))

    def recv_promise(self, promise: PromiseMsg, from_uid: int):
        # ignore if we already got this
        if from_uid in self.promises_rcvd:
            return
        # ignore if promise id is too old
        # print(promise, self.proposal)
        if self.proposal.id is not None and promise.id < self.proposal.id:
            return

        self.promises_rcvd.add(from_uid)

        if promise.accepted_id is not None:
            if self.proposal.id is None or promise.accepted_id > self.proposal.id:
                self.proposal = Proposal(promise.accepted_id, promise.accepted_value)

        if len(self.promises_rcvd) >= self.quorum_size:
            self.messenger.send_accept_request(
                AcceptRequestMsg(self.proposal.id, self.proposal.value)
            )

    def recv_accept(self, accept: AcceptMsg, from_uid: int):
        # idk what to do now - imo only learners need to know this
        pass

    def reset(self):
        self.proposal = Proposal(None, None)
        self.promises_rcvd = set()


class Acceptor:
    def __init__(self, uid: int, quorum_size: int, messenger: Messenger):
        self.uid = uid
        self.quorum_size = quorum_size
        self.messenger = messenger
        self.promised_id: int | None = None
        self.accepted_proposal: Proposal | None = None

    def __repr__(self) -> str:
        return f"{type(self).__name__}(uid={self.uid})"

    def recv_prepare(self, prepare: PrepareMsg, from_uid: int):
        # should we ignore this prepare msg?
        if self.promised_id is not None and prepare.id < self.promised_id:
            return

        self.promised_id = prepare.id

        # didnt accept anything yet
        if self.accepted_proposal is None:
            self.messenger.send_promise(PromiseMsg(self.promised_id), from_uid)
        else:
            self.messenger.send_promise(
                PromiseMsg(
                    self.promised_id,
                    self.accepted_proposal.id,
                    self.accepted_proposal.value,
                ),
                from_uid,
            )

    def recv_accept_request(self, accept_request: AcceptRequestMsg, from_uid: int):
        # shold we ignore this msg?
        if self.promised_id is not None and accept_request.id < self.promised_id:
            return

        self.promised_id = accept_request.id
        self.accepted_proposal = Proposal(accept_request.id, accept_request.value)
        self.messenger.send_accept(
            AcceptMsg(self.accepted_proposal.id, self.accepted_proposal.value), from_uid
        )

    def reset(self):
        self.promised_id = None
        self.accepted_proposal = None


class Learner:
    def __init__(self, uid: int, quorum_size: int, messenger: Messenger):
        self.uid = uid
        self.quorum_size = quorum_size
        self.messenger = messenger
        self.accept_store = AcceptStore(quorum_size)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(uid={self.uid})"

    def recv_accept(self, accept: AcceptMsg, from_uid: int):
        # are we done?
        if self.accept_store.get_consensus_value() is not None:
            return

        self.accept_store.add_new_proposal(accept, from_uid)

        # if we reached consensus broadcast this info
        consensus_value = self.accept_store.get_consensus_value()
        if consensus_value is not None:
            self.messenger.send_consensus_reached(consensus_value)

    def reset(self):
        self.accept_store.reset()
