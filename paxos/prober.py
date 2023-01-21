import argparse
import http
import logging
import random
import threading
import time
from threading import Thread
from urllib.parse import urljoin

import requests
from flask import Flask, jsonify
from marshmallow import ValidationError

from paxos.logic.communication import Network


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--probe-period", type=float, required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--leader-update-cb")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--workers", type=str, nargs="*")
    g.add_argument("--worker-ports", type=int, nargs="*")
    p.add_argument("-v", "--verbose", action="store_true")

    args = p.parse_args()

    logging.getLogger("werkzeug").setLevel(logging.WARN)
    logger = logging.getLogger("prober")

    if args.workers is not None:
        workers = args.workers
    elif args.worker_ports is not None:
        addrs_uids = Network.get_uids(
            f"http://localhost:{port}" for port in args.worker_ports
        )
        workers = {uid: addr for addr, uid in addrs_uids.items()}
    else:
        workers = {}

    mtx = threading.RLock()
    last_probed = None
    leader = None

    def elect_leader():
        while True:
            for uid, addr in workers.items():
                try:
                    logger.info(f"asking node[{uid}] ({addr}) to elect the leader")
                    resp = requests.post(f"{addr}/admin/elect_leader")
                    resp.raise_for_status()

                    data = resp.json()
                    with mtx:
                        nonlocal leader
                        leader = data["leader"]
                        if args.leader_update_cb is not None:
                            requests.put(
                                args.leader_update_cb,
                                json={"leader": leader},
                            )
                    return
                except:
                    pass

            time.sleep(1.0)

    if args.leader_update_cb is not None:
        elect_leader()

    def probe_thread_fn():
        while True:
            cur_addr = random.choice(list(workers.values()))
            with mtx:
                nonlocal last_probed
                last_probed = cur_addr

            try:
                req_url = urljoin(cur_addr, "/admin/healthcheck")
                resp = requests.get(req_url)
                resp.raise_for_status()
            except:
                if cur_addr == leader:
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
