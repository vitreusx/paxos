import logging
import pickle
import socket
import socketserver
import uuid
from logging import Logger
from pathlib import Path
from typing import Any, Iterable, Union

from paxos.logic import roles
from paxos.logic.communication import Communicator, Network, NodeID, PaxosMsg, Role
from paxos.logic.data import Payload
from paxos.logic.dictionary import WriteOnceDict
from paxos.logic.generator import TimeAwareIDGenerator
from paxos.utils.logging import format_payload


class UDP_Comm(Communicator):
    """Paxos communicator for Multi-Paxos."""

    def __init__(self, net: Network, key: Any, logger: Logger):
        self.net = net
        self.key = key
        self.log = logger.info

    def send(self, message: PaxosMsg, to: Iterable[NodeID]):
        payload = Payload(self.net.me.id, self.key, message)
        data = pickle.dumps(payload)

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            for recv_id in to:
                self.log(format_payload(payload, recv_id))
                host, port = self.net[recv_id].addr.split(":")
                port = int(port)
                sock.sendto(data, (host, port))

    def all_of(self, role: Role) -> list[NodeID]:
        return [node.id for node in self.net.all_of(role)]


class MultiPaxos(WriteOnceDict):
    """A Multi-Paxos setup, i.e. a dictionary of sorts, with each key being assigned a writeable-once value agreed on by consensus through Paxos."""

    def __init__(self, net: Network, save_path: Union[str, Path]):
        self.net = net
        self.instances: dict[Any, roles.Server] = {}
        self.logger = logging.getLogger(f"node[{self.net.me.id}]")
        self.save_path = Path(save_path)

        if self.save_path.exists():
            with open(self.save_path, mode="rb") as save_f:
                self.state = pickle.load(save_f)

    @property
    def state(self) -> dict:
        return {key: inst.state for key, inst in self.instances.items()}

    @state.setter
    def state(self, value: dict):
        for key, inst_state in value.items():
            comm = UDP_Comm(self.net, key, self.logger)
            self.instances[key] = self._create_server(comm)
            self.instances[key].state = inst_state

    def _create_server(self, comm: Communicator) -> roles.Server:
        uid = self.net.me.id
        max_uid = max(self.net.nodes.keys()) + 1
        id_generator = TimeAwareIDGenerator(uid, max_uid)
        return roles.Server(comm, id_generator)

    def _lookup(self, key: Any) -> roles.Server:
        if key not in self.instances:
            comm = UDP_Comm(self.net, key, self.logger)
            server_inst = self._create_server(comm)
            self.instances[key] = server_inst
        return self.instances[key]

    def _commit(self):
        with open(self.save_path, mode="wb") as save_f:
            pickle.dump(self.state, save_f)

    def UDP_Server(self) -> socketserver.UDPServer:
        paxos = self

        class Handler(socketserver.DatagramRequestHandler):
            def handle(self):
                payload: Payload = pickle.load(self.rfile)
                # paxos.log(payload)

                paxos_inst = paxos._lookup(payload.key)
                paxos_inst.on_recv(payload.sender, payload.message)

                paxos._commit()

        host, port = self.net.me.addr.split(":")
        port = int(port)
        return socketserver.UDPServer((host, port), Handler)

    async def set(self, key: Any, value: Any) -> tuple[bool, Any]:
        """Propose a value to be associated with a given key. Returns the final value reached by consensus (which may or may not be the proposed value)."""

        paxos_inst = self._lookup(key)
        if paxos_inst.proposer.value is not None:
            set_uid, set_value = paxos_inst.proposer.value
            return False, set_value

        event = paxos_inst.proposer.value_set_ev

        uid = uuid.uuid4()
        timeout = 1.0
        while True:
            paxos_inst.proposer.request((uid, value))
            if event.wait(timeout):
                assert paxos_inst.proposer.value
                set_uid, set_value = paxos_inst.proposer.value
                return set_uid == uid, set_value

            timeout *= 2.0

    async def __getitem__(self, key: Any) -> Any:
        """Get the value associated with a given key. If consensus has not yet been reached on what should be the value, None is returned."""

        paxos_inst = self._lookup(key)
        if paxos_inst.questioner.value is not None:
            set_uid, set_value = paxos_inst.questioner.value
            return set_value

        event = paxos_inst.questioner.value_set_ev

        timeout = 1.0
        while True:
            paxos_inst.questioner.query()
            if event.wait(timeout):
                if paxos_inst.questioner.value is not None:
                    set_uid, set_value = paxos_inst.questioner.value
                    return set_value
                else:
                    return None
            else:
                timeout *= 2.0
