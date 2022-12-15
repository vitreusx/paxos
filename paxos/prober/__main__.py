import threading
from threading import Thread
import argparse
import random
import time
import requests
import jinja2
from pathlib import Path
import os
import signal
from flask import Flask, request, jsonify, g
from marshmallow import Schema, fields, ValidationError
import http
import tempfile
import subprocess
import atexit
import logging
from multiprocessing import Process
from urllib.parse import urlparse, urljoin


logging.basicConfig(level=logging.INFO)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--probe-period", type=float, required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--gateway-pid", type=int)
    p.add_argument("--gateway-conf")
    p.add_argument("--gateway-port", type=int)
    g = p.add_mutually_exclusive_group()
    g.add_argument("--workers", type=str, nargs="*")
    g.add_argument("--worker-ports", type=int, nargs="*")

    args = p.parse_args()

    logging.getLogger("werkzeug").setLevel(logging.WARN)

    if args.workers is not None:
        workers = args.workers
    elif args.worker_ports is not None:
        workers = [f"http://localhost:{port}" for port in args.worker_ports]
    else:
        workers = []

    mtx = threading.RLock()
    last_probed = None
    leader = None

    if args.gateway_port is not None:
        script_dir = Path(__file__).parent
        j2_loader = jinja2.FileSystemLoader(script_dir)
        j2_env = jinja2.Environment(loader=j2_loader)
        nginx_conf_j2 = j2_env.get_template("nginx.conf.j2")

        gateway_conf_path = Path(args.gateway_conf)

        def update_conf():
            conf_args = {"leader": leader, "gateway_port": args.gateway_port}
            conf_txt = nginx_conf_j2.render(conf_args)
            with open(gateway_conf_path, mode="w") as gateway_conf_f:
                gateway_conf_f.write(conf_txt)
            os.kill(args.gateway_pid, signal.SIGHUP)

    def elect_leader():
        while True:
            for addr in workers:
                other_nodes = list(workers)
                other_nodes.remove(addr)
                payload = {"other_nodes": other_nodes}

                try:
                    resp = requests.post(f"{addr}/admin/elect_leader", json=payload)
                    resp.raise_for_status()

                    data = resp.json()
                    with mtx:
                        nonlocal leader
                        leader = data["leader"]
                        logging.info(f"Elected leader {leader}")
                        if args.gateway_pid is not None:
                            update_conf()
                    return
                except:
                    pass

            time.sleep(1.0)

    if args.gateway_port is not None:
        elect_leader()

    def probe_thread_fn():
        while True:
            cur_addr = random.choice(workers)
            with mtx:
                nonlocal last_probed
                last_probed = cur_addr

            try:
                req_url = urljoin(cur_addr, "/admin/healthcheck")
                resp = requests.get(req_url)
                resp.raise_for_status()
            except:
                logging.info(f"Node {cur_addr} died [Leader is {leader}]")
                if cur_addr == leader:
                    logging.info(f"Leader died")
                    elect_leader()

            time.sleep(args.probe_period)

    probe_thr = Thread(target=probe_thread_fn)
    probe_thr.start()

    app = Flask(__name__)

    @app.errorhandler(ValidationError)
    def on_validation_error(error: ValidationError):
        code = http.HTTPStatus.BAD_REQUEST
        resp = jsonify({"error": "ValidationError", "details": error.messages})
        return resp, code

    @app.get("/status")
    def status():
        with mtx:
            return {"last_probed": last_probed}

    @app.get("/leader")
    def get_leader():
        with mtx:
            if leader is None:
                elect_leader()
            return {"leader": leader}

    app.run(debug=False, port=args.port)


if __name__ == "__main__":
    main()
