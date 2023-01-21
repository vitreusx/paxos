import argparse
import logging
import os
import signal
import subprocess
import tempfile
import threading
from pathlib import Path
from subprocess import DEVNULL
from typing import List

import jinja2
import scipy

from paxos.deploy.killer.interactive import InteractiveKiller
from paxos.deploy.killer.random import RandomKiller
from paxos.deploy.sockets import SocketSet
from paxos.deploy.worker import PaxosWorker


class Leaderless:
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

        g = p.add_mutually_exclusive_group()
        g.add_argument("--num-workers", type=int)
        p.add_argument("--gateway-port", type=int)
        p.add_argument("-v", "--verbose", action="store_true")
        p.add_argument("--killer-port", type=int)
        p.add_argument("--killer-type", type=str, choices=["interactive", "random"])
        p.add_argument("--generator", type=str, choices=["incremental", "time_aware"])

        return p.parse_args()

    def setup_logging(self):
        logging.getLogger("werkzeug").setLevel(logging.WARN)

    def reserve_ports(self):
        with SocketSet() as sset:
            if self.args.gateway_port is not None:
                sset.reserve(port=self.args.gateway_port)
                _, self.flask_port = sset.reserve()

            self.flask_ports, self.comm_ports = [], []
            self.worker_addrs = []
            for _ in range(self.args.num_workers):
                _, flask_port = sset.reserve()
                self.flask_ports.append(flask_port)
                _, comm_port = sset.reserve()
                self.comm_ports.append(comm_port)
                self.worker_addrs.append(f"localhost:{flask_port}")

    def create_workers(self):
        self.workers = []
        comm_net = [f"localhost:{p}" for p in self.comm_ports]
        self.paxos_dir = tempfile.TemporaryDirectory()
        for flask_p, comm_p in zip(self.flask_ports, self.comm_ports):
            worker = PaxosWorker(
                mode="leaderless",
                flask_port=flask_p,
                comm_port=comm_p,
                ledger_file=self.args.ledger_file,
                comm_net=comm_net,
                verbose=self.args.verbose,
                paxos_dir=self.paxos_dir.name,
                generator_type=self.args.generator,
            )
            self.workers.append(worker)
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
                worker_addrs=self.worker_addrs,
            )
        )
        self.gateway_conf.close()

        self.gateway = subprocess.Popen(
            ["nginx", "-g", "daemon off;", "-c", self.gateway_conf.name], stdout=DEVNULL
        )

    def update_gateway(self, leader):
        conf_txt = self.nginx_conf_j2.render(
            gateway_port=self.args.gateway_port,
            worker_addrs=self.worker_addrs,
        )
        with open(self.gateway_conf.name, "w") as conf_f:
            conf_f.write(conf_txt)

        os.kill(self.gateway.pid, signal.SIGHUP)

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

    def setup_signals(self):
        def handler(signo, frame):
            self.finishing.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, handler)

    def cleanup(self):
        if self.args.gateway_port is not None:
            self.gateway.terminate()
            self.gateway.wait()

        if self.args.kill_every is not None:
            self.killer.join()

        for worker in self.workers:
            worker.kill()

    def main(self):
        self.finishing = threading.Event()

        self.args = self.parse_args()
        self.setup_logging()

        self.reserve_ports()

        self.create_workers()

        if self.args.gateway_port is not None:
            self.create_gateway()

        if self.args.kill_every is not None:
            self.create_killer()

        self.setup_signals()

        self.finishing.wait()

        self.cleanup()


if __name__ == "__main__":
    app = Leaderless()
    app.main()
