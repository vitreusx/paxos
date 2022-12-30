import argparse
import logging
import os
import random
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from multiprocessing import Process
from pathlib import Path
from subprocess import DEVNULL
from threading import Thread

import jinja2
from flask import Flask, request
from marshmallow import Schema, fields


def get_socket(host="", port=0):
    """Get a socket.
    :param host: Hostname, pass "" for localhost.
    :param port: Port to bind, pass 0 to pick free port."""

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    return sock


def port_of_socket(sock: socket.socket):
    """Get port for the socket."""
    return sock.getsockname()[1]


@contextmanager
def reserved_sockets():
    """Maintain a list of sockets, and free them upon leaving context. Useful when trying to RSVP a bunch of free ports."""
    try:
        sockets = []
        yield sockets
    finally:
        for sock in sockets:
            sock.close()


def main():
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

    g = p.add_mutually_exclusive_group()
    g.add_argument("--num-workers", type=int)
    g.add_argument("--worker-ports", type=int, nargs="*")

    p.add_argument("--gateway-port", type=int)

    p.add_argument("-v", "--verbose", action="store_true")

    args = p.parse_args()

    # Set up the logger - by default INFO-level stuff is suppressed.
    logging.getLogger("werkzeug").setLevel(logging.WARN)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARN)

    comm_ports = []
    with reserved_sockets() as rsvd:
        if args.gateway_port is not None:
            gateway_sock = get_socket(port=args.gateway_port)
            rsvd.append(gateway_sock)

        if args.prober_port is not None:
            prober_sock = get_socket(port=args.prober_port)
            rsvd.append(prober_sock)

        if args.worker_ports is not None:
            for worker_port in args.worker_ports:
                worker_sock = get_socket(port=worker_port)
                rsvd.append(worker_sock)

        if args.prober_port is not None:
            prober_port = args.prober_port
        else:
            prober_sock = get_socket()
            prober_port = port_of_socket(prober_sock)
            rsvd.append(prober_sock)

        if args.worker_ports is not None:
            worker_ports = args.worker_ports
        elif args.num_workers is not None:
            worker_ports = []
            for _ in range(args.num_workers):
                worker_sock = get_socket()
                worker_ports.append(port_of_socket(worker_sock))
                rsvd.append(worker_sock)
            worker_ports = sorted(worker_ports)
        else:
            worker_ports = []
        
        for _ in worker_ports:
            comm_sock = get_socket()
            comm_port = port_of_socket(comm_sock)
            rsvd.append(comm_sock)
            comm_ports.append(comm_port)
        
        flask_sock = get_socket()
        flask_port = port_of_socket(flask_sock)
        rsvd.append(flask_sock)

    def parse_bounds(bounds):
        if bounds is not None:
            if len(bounds) > 1:
                avg, max_dev = bounds[:2]
                assert avg - max_dev > 0
                return (avg - max_dev, avg + max_dev)
            else:
                avg = bounds[0]
                return (avg, avg)
        else:
            return None

    kill_every = parse_bounds(args.kill_every)
    restart_after = parse_bounds(args.restart_after)

    ledger_file = Path(args.ledger_file).absolute()

    worker_addrs = [f"http://localhost:{p}" for p in worker_ports]
    other_addrs = {
        port: {*worker_addrs} - {f"http://localhost:{port}"} for port in worker_ports
    }

    def spawn_worker(port: int):
        return subprocess.Popen(
            [
                "python3",
                "-m",
                "paxos.worker",
                "--flask-port",
                str(port),
                "--ledger-file",
                str(ledger_file),
                *(["-v"] if args.verbose else []),
                "--other-nodes",
                *other_addrs[port],
            ],
            stdin=DEVNULL,
            stdout=DEVNULL,
        )

    if args.gateway_port is not None:
        gateway_conf = tempfile.NamedTemporaryFile(mode="w", delete=False)

        script_dir = Path(__file__).parent
        j2_loader = jinja2.FileSystemLoader(script_dir)
        j2_env = jinja2.Environment(loader=j2_loader)
        nginx_conf_j2 = j2_env.get_template("nginx.conf.j2")

        conf_txt = nginx_conf_j2.render(
            gateway_port=args.gateway_port,
            leader=None,
        )
        gateway_conf.write(conf_txt)
        gateway_conf.close()

        gateway_proc = subprocess.Popen(
            ["nginx", "-g", "daemon off;", "-c", gateway_conf.name],
            stdout=DEVNULL,
            stderr=DEVNULL,
        )

        logging.info(f"Running gateway on http://localhost:{args.gateway_port}")
        logging.info(f"Gateway args: {gateway_proc.args}")

    app = Flask(__name__)

    class UpdateLeaderSchema(Schema):
        leader = fields.Str()

    @app.put("/leader")
    def update_leader():
        data = UpdateLeaderSchema().load(request.json)

        conf_txt = nginx_conf_j2.render(
            gateway_port=args.gateway_port,
            leader=data["leader"],
        )
        with open(gateway_conf.name, "w") as conf_f:
            conf_f.write(conf_txt)

        os.kill(gateway_proc.pid, signal.SIGHUP)

        return {}

    def flask_fn():
        sys.stdout = open(os.devnull, "w")
        app.run(use_reloader=False, debug=False, port=flask_port)

    flask_proc = Process(target=flask_fn)
    flask_proc.start()

    workers = []
    for port in worker_ports:
        proc = spawn_worker(port)
        workers.append({"port": port, "proc": proc, "alive": True})

    logging.info(f"Workers: {worker_addrs}")

    finishing = threading.Event()
    any_alive_cv = threading.Condition()

    def killer_fn():
        timers_mtx = threading.Lock()
        timers = {}
        timer_id = 0

        while not finishing.is_set():
            with any_alive_cv:
                any_alive_cv.wait_for(
                    lambda: any(w["alive"] for w in workers) or finishing.is_set()
                )
                if finishing.is_set():
                    break

                worker_idx = random.choice(
                    [idx for idx, w in enumerate(workers) if w["alive"]]
                )

            worker = workers[worker_idx]
            worker_info = {"pid": worker["proc"].pid, "port": worker["port"]}
            worker["proc"].terminate()
            worker["alive"] = False
            logging.info(f"Terminated worker {worker_info}")

            if restart_after is not None:
                min_delay, max_delay = restart_after
                delay = min_delay + (max_delay - min_delay) * random.random()

                def restart_fn(worker_idx_, timer_id_):
                    worker = workers[worker_idx_]
                    with any_alive_cv:
                        proc = spawn_worker(worker["port"])
                        worker["proc"] = proc
                        worker["alive"] = True

                        worker_info = {
                            "pid": worker["proc"].pid,
                            "port": worker["port"],
                        }
                        logging.info(f"Restarted worker {worker_info}")

                        any_alive_cv.notify()

                    with timers_mtx:
                        del timers[timer_id_]

                with timers_mtx:
                    restart_timer = threading.Timer(
                        delay,
                        restart_fn,
                        args=(
                            worker_idx,
                            timer_id,
                        ),
                    )
                    restart_timer.start()
                    timers[timer_id] = restart_timer
                    timer_id += 1

            min_wait, max_wait = kill_every
            wait_s = min_wait + (max_wait - min_wait) * random.random()
            time.sleep(wait_s)

        with timers_mtx:
            for timer in timers.values():
                timer.cancel()
                timer.join()

    prober_argv = ["python3", "-m", "paxos.prober"]
    prober_argv.extend(["--probe-period", str(args.probe_period)])
    prober_argv.extend(["--port", str(prober_port)])
    prober_argv.extend(["--worker-ports", *(str(w["port"]) for w in workers)])
    if args.gateway_port is not None:
        leader_url = f"http://localhost:{flask_port}/leader"
        prober_argv.extend(["--leader-url", leader_url])
    if args.verbose:
        prober_argv.extend(["-v"])

    prober_proc = subprocess.Popen(prober_argv, stdout=DEVNULL, stdin=DEVNULL)
    logging.info(f"Running prober on http://localhost:{prober_port}")

    killer = None
    if kill_every is not None:
        killer = Thread(target=killer_fn)
        killer.start()

    def handler(signo, frame):
        finishing.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handler)

    logging.info("Press Ctrl-C to stop all the processes.")

    finishing.wait()

    if args.gateway_port is not None:
        gateway_proc.kill()
        gateway_proc.wait()

    prober_proc.terminate()
    prober_proc.wait()

    flask_proc.terminate()
    flask_proc.join()

    if killer is not None:
        with any_alive_cv:
            any_alive_cv.notify()
        killer.join()

    for worker in workers:
        if worker["alive"]:
            worker["proc"].terminate()
            worker["proc"].wait()


if __name__ == "__main__":
    main()
