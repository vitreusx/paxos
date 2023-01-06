from .comm import *
from .server import *
import socket
import pickle
from pathlib import Path
import socketserver
from typing import Optional
from threading import Event
from paxos.utils.atomic import *
import asyncio


@dataclass
class Payload:
    sender: int
    key: Any
    message: PaxosMsg


class UDP_KV_Comm(Communicator):
    """Paxos communicator for the key-value store."""

    def __init__(self, net: Network, key: Any):
        self.net = net
        self.key = key

    def send(self, message: PaxosMsg, to: Iterable[NodeID]):
        payload = Payload(self.net.me.id, self.key, message)
        data = pickle.dumps(payload)

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            for recv_id in to:
                host, port = self.net[recv_id].addr.split(":")
                port = int(port)
                sock.sendto(data, (host, port))

    def all_of(self, role: Role) -> Iterable[NodeID]:
        return [node.id for node in self.net.all_of(role)]


class KeyValueStore:
    """A write-once key-value store with values set by consensus with Paxos."""

    def __init__(self, net: Network, save_path=None, log=None):
        self.net = net
        self.instances: dict[Any, PaxosServer] = {}
        self.value_set_events: dict[Any, Event] = {}
        self.log = log

        self.save_path = Path(save_path) if save_path is not None else None
        if self.save_path is not None and self.save_path.exists():
            with open(self.save_path, "rb") as save_f:
                self.state = pickle.load(save_f)

    def _lookup(self, key: Any) -> PaxosServer:
        if key not in self.instances:
            comm = UDP_KV_Comm(self.net, key)
            paxos_srv = PaxosServer(comm)
            self.instances[key] = paxos_srv
        return self.instances[key]

    @property
    def state(self):
        return {key: server.state for key, server in self.instances.items()}

    @state.setter
    def state(self, value):
        for key, server_state in value.items():
            paxos_srv = self._lookup(key)
            paxos_srv.state = server_state

    def UDP_KV_Server(self):
        kv_store = self

        class Handler(socketserver.DatagramRequestHandler):
            def handle(self):
                payload: Payload = pickle.load(self.rfile)
                if kv_store.log:
                    kv_store.log(f"[from {payload.sender}] {payload.message}")

                paxos_srv = kv_store._lookup(payload.key)
                for behavior in paxos_srv.behaviors:
                    behavior.on_recv(payload.sender, payload.message)

                kv_store.commit()

                if payload.key in kv_store.value_set_events:
                    event = kv_store.value_set_events[payload.key]
                    event.set()

        host, port = self.net.me.addr.split(":")
        port = int(port)
        return socketserver.UDPServer((host, port), Handler)

    def commit(self):
        if self.save_path is not None:
            atomic_save(pickle.dumps(self.state), self.save_path)

    async def set(self, key: Any, value: Any):
        timeout = 1.0
        while True:
            paxos_inst = self._lookup(key)
            self.value_set_events[key] = Event()
            event = self.value_set_events[key]

            paxos_inst.proposer.request(value)
            loop = asyncio.get_running_loop()
            try:
                fut = loop.run_in_executor(None, event.wait)
                await asyncio.wait_for(fut, timeout)
                return self
            except asyncio.exceptions.TimeoutError:
                timeout *= 2.0

    async def __getitem__(self, key: Any) -> Any:
        await self.set(key, None)
        return self._lookup(key).learner.value
