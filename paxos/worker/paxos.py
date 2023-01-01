from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Iterable, Tuple, Literal, get_args
from abc import ABC, abstractmethod
import pickle
import socket
import socketserver
from contextlib import contextmanager
import asyncio
import logging
import threading
from threading import Event


@dataclass
class Request:
    value: Any


@dataclass
class Prepare:
    id: int


@dataclass
class Accepted:
    id: int
    value: Any


@dataclass
class Promise:
    id: int
    prev: Accepted | None


@dataclass
class Accept:
    id: int
    value: Any


PaxosMsg = Request | Prepare | Promise | Accept | Accepted
RoleName = Literal["Proposer", "Acceptor", "Learner"]
Address = str


class Comm(ABC):
    @abstractmethod
    def send(self, message: PaxosMsg, to: Iterable[int]):
        """Send Paxos message to a number of other nodes."""

    @abstractmethod
    def all_of(self, role: RoleName) -> Iterable[int]:
        """Get UIDs of all nodes in the network with a given role."""

    @property
    def acceptors(self):
        return self.all_of("Acceptor")

    @property
    def learners(self):
        return self.all_of("Learner")


class Role(ABC):
    @abstractmethod
    def recv(self, sender: int, message: PaxosMsg):
        """Perform an action on receiving a message."""


@dataclass
class Proposal:
    id: int
    value: Any
    acceptor_ids: set[int] = field(default_factory=lambda: set())
    prev_accepted: list[Accepted] = field(default_factory=lambda: [])


class Proposer(Role):
    def __init__(self, comm: Comm, quorum_size: int):
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

    def recv(self, sender: int, message: PaxosMsg):
        if isinstance(message, Request):
            self.recv_request(message)
        elif isinstance(message, Promise):
            self.recv_promise(sender, message)
        elif isinstance(message, Accepted):
            self.recv_accepted(message)


class Acceptor(Role):
    def __init__(self, comm: Comm):
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

    def recv(self, sender: int, message: PaxosMsg):
        if isinstance(message, Prepare):
            self.recv_prepare(sender, message)
        elif isinstance(message, Accept):
            self.recv_accept(sender, message)


class Learner(Role):
    def __init__(self, comm: Comm):
        self.comm = comm
        self.value = None
        self.value_set_ev = Event()

    def recv_accepted(self, accepted: Accepted):
        self.value = accepted.value
        if self.value_set_ev is not None:
            self.value_set_ev.set()

    def recv(self, sender: int, message: PaxosMsg):
        if isinstance(message, Accepted):
            self.recv_accepted(message)


@dataclass
class Node:
    uid: int
    addr: Address
    roles: set[RoleName]


@dataclass
class Network:
    nodes: dict[int, Node]

    @staticmethod
    def from_addrs(addrs: Iterable[Address]) -> Network:
        addrs_ids = [(addr, idx) for idx, addr in enumerate(addrs)]
        addrs_ids = sorted(addrs_ids, key=lambda x: x[0])

        all_roles = {*get_args(RoleName)}
        nodes = {uid: Node(uid, addr, all_roles) for addr, uid in addrs_ids}
        return Network(nodes)

    def all_of(self, role: RoleName):
        return [node for node in self.nodes.values() if role in node.roles]

    def __getitem__(self, uid: int) -> Node:
        return self.nodes[uid]

    def __len__(self) -> int:
        return len(self.nodes)

    def lookup(self, addr: Address) -> Node:
        for node in self.nodes.values():
            if node.addr == addr:
                return node

        raise RuntimeError(f"Node {addr} not found in the network!")


@dataclass
class Payload:
    sender: int
    key: Any
    message: PaxosMsg


class UDP_Comm(Comm):
    def __init__(self, net: Network, uid: int, key: Any):
        self.net = net
        self.uid = uid
        self.key = key

    def send(self, message: PaxosMsg, to: Iterable[int]):
        payload = Payload(self.uid, self.key, message)
        data = pickle.dumps(payload)

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            for recv_uid in to:
                host, port = self.net[recv_uid].addr.split(":")
                port = int(port)
                sock.sendto(data, (host, port))

    def all_of(self, role: RoleName) -> Iterable[int]:
        return [node.uid for node in self.net.all_of(role)]


class PaxosInstance:
    def __init__(
        self,
        net: Network,
        addr: Address,
        key: Any,
    ):
        self.node = net.lookup(addr)
        self.comm = UDP_Comm(net, self.node.uid, key)

        self.roles: list[Role] = []
        for role in self.node.roles:
            match role:
                case "Acceptor":
                    self.acceptor = Acceptor(self.comm)
                    self.roles.append(self.acceptor)
                case "Proposer":
                    quorum_size = len(net.all_of("Acceptor")) // 2 + 1
                    self.proposer = Proposer(self.comm, quorum_size)
                    self.roles.append(self.proposer)
                case "Learner":
                    self.learner = Learner(self.comm)
                    self.roles.append(self.learner)


class KeyValueStore:
    def __init__(self, net: Network, addr: Address):
        self.net = net
        self.addr = addr
        self.node = self.net.lookup(self.addr)
        self.instances: dict[Any, PaxosInstance] = {}

    def _paxos_inst(self, key: Any):
        if key not in self.instances:
            paxos_inst = PaxosInstance(self.net, self.addr, key)
            self.instances[key] = paxos_inst
        return self.instances[key]

    def UDP_Server(self):
        kv_store = self

        class Handler(socketserver.DatagramRequestHandler):
            def handle(self):
                payload: Payload = pickle.load(self.rfile)
                logging.info(
                    f"[{payload.sender} -> {kv_store.node.uid}] {payload.message}"
                )

                paxos_inst = kv_store._paxos_inst(payload.key)
                for role in paxos_inst.roles:
                    role.recv(payload.sender, payload.message)

        host, port = self.addr.split(":")
        port = int(port)
        return socketserver.UDPServer((host, port), Handler)

    async def set(self, key: Any, value: Any):
        paxos_inst = self._paxos_inst(key)
        value_set_ev = paxos_inst.learner.value_set_ev
        value_set_ev.clear()
        paxos_inst.proposer.request(value)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, value_set_ev.wait)
        return self

    async def __getitem__(self, key: Any) -> Any:
        await self.set(key, None)
        learner = self._paxos_inst(key).learner
        return learner.value
