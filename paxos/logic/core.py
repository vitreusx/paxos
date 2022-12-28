from __future__ import annotations

from typing import Set

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
        self, quorum_size: int, id_generator: IDGenerator, messenger: Messenger
    ):
        self.quorum_size = quorum_size
        self.id_generator = id_generator
        self.messenger = messenger
        self.proposal: Proposal | None = None
        self.promises_rcvd: Set[str] = set()

    def propose(self, value: str):
        if self.proposal is not None:
            self.proposal.value = value
        self.prepare()

    def prepare(self):
        id = self.id_generator.new_id()
        self.messenger.send_prepare(PrepareMsg(id))

    def recv_promise(self, promise: PromiseMsg, from_id: str):

        # ignore if we already got this or id is too old
        if from_id in self.promises_rcvd or promise.id != self.proposal.id:
            return

        self.promises_rcvd.add(from_id)

        if promise.accepted_id is not None and promise.accepted_id > self.proposal.id:
            self.proposal.id = promise.accepted_id
            self.proposal.value = promise.accepted_value

        if len(self.promises_rcvd) >= self.quorum_size:
            self.messenger.send_accept(
                AcceptMsg(self.proposal.id, self.proposal.value), from_id
            )

    def reset(self):
        self.proposal = None
        self.promises_rcvd = set()


class Acceptor:
    def __init__(self, quorum_size: int, messenger: Messenger):
        self.quorum_size = quorum_size
        self.messenger = messenger
        self.promised_id: int | None = None
        self.accepted_proposal: Proposal | None = None

    def recv_prepare(self, prepare: PrepareMsg, from_id: str):
        # should we ignore this prepare msg?
        if self.promised_id is not None and prepare.id < self.promised_id:
            return

        self.promised_id = prepare.id

        # have we accepted anything already?
        if self.accepted_proposal is not None:
            self.messenger.send_promise(
                PromiseMsg(
                    self.promised_id,
                    self.accepted_proposal.id,
                    self.accepted_proposal.value,
                ),
                from_id,
            )
        else:
            self.messenger.send_promise(PromiseMsg(self.promised_id), from_id)

    def recv_accept_request(self, accept_request: AcceptRequestMsg, from_id: str):
        # shold we ignore this msg?
        if self.promised_id is not None and accept_request.id < self.promised_id:
            return

        self.promised_id = accept_request.id
        self.accepted_proposal = Proposal(accept_request.id, accept_request.value)
        self.messenger.send_accept(
            AcceptMsg(self.accepted_proposal.id, self.accepted_proposal.value), from_id
        )

    def reset(self):
        self.promised_id = None
        self.accepted_proposal = None


class Learner:
    def __init__(self, quorum_size: int, messenger: Messenger):
        self.quorum_size = quorum_size
        self.messenger = messenger
        self.accept_store = AcceptStore(quorum_size)

    def recv_accept(self, accept: AcceptMsg, from_id: str):
        # are we done?
        if self.accept_store.get_consensus_value() is not None:
            return

        self.accept_store.add_new_proposal(accept, from_id)

        # if we reached consensus broadcast this info
        consensus_value = self.accept_store.get_consensus_value()
        if consensus_value is not None:
            self.messenger.send_consensus_reached(consensus_value)
