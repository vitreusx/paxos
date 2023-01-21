import argparse
import logging
import os
import signal
import subprocess
import sys
import tempfile
import threading
from multiprocessing import Process
from pathlib import Path
from subprocess import DEVNULL
from typing import List

import jinja2
import scipy.stats
from flask import Flask, request
from marshmallow import Schema, fields

from paxos.deploy.killer.interactive import InteractiveKiller
from paxos.deploy.killer.random import RandomKiller
from paxos.deploy.sockets import SocketSet
from paxos.deploy.worker import PaxosWorker
from paxos.logic.communication import Network


class WithLeader:
    def parse_args(self):
        p = argparse.ArgumentParser()

        p.add_argument(
            "--kill-every",
            type=float,
            nargs="+",
            metavar=("MEAN", "MAX_DEV"),
        )
        p.add_argument(
            "--restart-after",
            type=float,
            nargs="+",
            metavar=("MEAN", "MAX_DEV"),
        )
        p.add_argument("--ledger-file", required=True)
        p.add_argument("--prober-port", type=int)
        p.add_argument("--probe-period", type=float, required=True)
        p.add_argument("--num-workers", type=int)
        p.add_argument("--gateway-port", type=int)
        p.add_argument("-v", "--verbose", action="store_true")
        p.add_argument("--killer-port", type=int)
        p.add_argument("--killer-type", type=str, choices=["interactive", "random"])

        return p.parse_args()

    def setup_logging(self):
        # Set up the logger - by default INFO-level stuff is suppressed.
        logging.getLogger("werkzeug").setLevel(logging.WARN)

    def reserve_ports(self):
        with SocketSet() as sset:
            if self.args.gateway_port is not None:
                sset.reserve(port=self.args.gateway_port)
                _, self.flask_port = sset.reserve()

            _, self.prober_port = sset.reserve(port=self.args.prober_port)

            self.flask_ports, self.comm_ports = [], []
            for _ in range(self.args.num_workers):
                _, flask_port = sset.reserve()
                self.flask_ports.append(flask_port)
                _, comm_port = sset.reserve()
                self.comm_ports.append(comm_port)

    def create_workers(self):
        self.workers = {}
        comm_net = [f"localhost:{p}" for p in self.comm_ports]
        self.paxos_dir = tempfile.TemporaryDirectory()
        uids = Network.get_uids(self.comm_ports)
        for flask_p, comm_p in zip(self.flask_ports, self.comm_ports):
            worker = PaxosWorker(
                mode="with_leader",
                flask_port=flask_p,
                comm_port=comm_p,
                ledger_file=self.args.ledger_file,
                comm_net=comm_net,
                verbose=self.args.verbose,
                paxos_dir=self.paxos_dir.name,
            )
            uid = uids[comm_p]
            self.workers[uid] = worker
            worker.respawn()

    def create_gateway(self):
        self.gateway_conf = tempfile.NamedTemporaryFile(mode="w", delete=False)

        script_dir = Path(__file__).parent
        j2_loader = jinja2.FileSystemLoader(script_dir)
        j2_env = jinja2.Environment(loader=j2_loader)
        self.nginx_conf_j2 = j2_env.get_template("nginx.conf.j2")

        self.gateway_conf.write(
            self.nginx_conf_j2.render(
                gateway_port=self.args.gateway_port,
                leader=None,
            )
        )
        self.gateway_conf.close()

        self.gateway = subprocess.Popen(
            ["nginx", "-g", "daemon off;", "-c", self.gateway_conf.name], stdout=DEVNULL
        )

    def update_gateway(self, leader):
        conf_txt = self.nginx_conf_j2.render(
            gateway_port=self.args.gateway_port, leader=leader
        )
        with open(self.gateway_conf.name, "w") as conf_f:
            conf_f.write(conf_txt)

        os.kill(self.gateway.pid, signal.SIGHUP)

    def create_flask_app(self):
        app = Flask(__name__)

        class UpdateLeaderSchema(Schema):
            leader = fields.Str()

        @app.put("/leader")
        def update_leader():
            data = UpdateLeaderSchema().load(request.json)
            self.update_gateway(data["leader"])
            return {}

        def flask_proc_fn():
            sys.stdout = open(os.devnull, "w")
            app.run(use_reloader=False, debug=False, port=self.flask_port)

        self.flask_srv = Process(target=flask_proc_fn)
        self.flask_srv.start()

    def get_delay_rv(self, params: List[float]):
        if len(params) > 1:
            mean, max_dev = params[:2]
            loc, scale = mean - max_dev, 2 * max_dev
        else:
            val = params[0]
            loc, scale = val, 0.0
        return scipy.stats.uniform(loc=loc, scale=scale)

    def create_killer(self):
        if self.args.killer_type == "interactive":
            self.killer = InteractiveKiller(
                self.workers, self.finishing, self.args.killer_port
            )
        else:
            kill_every = self.get_delay_rv(self.args.kill_every)
            if self.args.restart_after is not None:
                restart_after = self.get_delay_rv(self.args.restart_after)
            else:
                restart_after = None
            self.killer = RandomKiller(
                list(self.workers.values()), self.finishing, kill_every, restart_after
            )
        self.killer.start()

    def create_prober(self):
        prober_argv = ["python3", "-m", "paxos.prober"]
        prober_argv.extend(["--probe-period", str(self.args.probe_period)])
        prober_argv.extend(["--port", str(self.prober_port)])
        prober_argv.extend(["--worker-ports", *(str(p) for p in self.flask_ports)])
        if self.args.gateway_port is not None:
            leader_update_cb = f"http://localhost:{self.flask_port}/leader"
            prober_argv.extend(["--leader-update-cb", leader_update_cb])
        if self.args.verbose:
            prober_argv.extend(["-v"])

        self.prober = subprocess.Popen(prober_argv, stdout=DEVNULL, stdin=DEVNULL)

    def setup_signals(self):
        def handler(signo, frame):
            self.finishing.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, handler)

    def cleanup(self):
        self.prober.terminate()
        self.prober.wait()

        if self.args.gateway_port is not None:
            self.flask_srv.terminate()
            self.flask_srv.join()

            self.gateway.terminate()
            self.gateway.wait()

        # if self.args.kill_every is not None:
        self.killer.join()

        for worker in self.workers.values():
            worker.kill()

    def main(self):
        self.finishing = threading.Event()

        self.args = self.parse_args()
        self.setup_logging()

        self.reserve_ports()

        if self.args.gateway_port is not None:
            self.create_gateway()
            self.create_flask_app()

        self.create_workers()

        self.create_prober()

        if self.args.kill_every is not None:
            self.create_killer()

        self.setup_signals()

        self.finishing.wait()

        self.cleanup()


if __name__ == "__main__":
    app = WithLeader()
    app.main()
