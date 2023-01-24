import argparse
import asyncio
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


def get_election_result(
    node_id: int, addr: str, election_id: int, logger: logging.Logger
) -> str | None:
    try:
        logger.info(f"asking node[{node_id}] ({addr}) to elect the leader")
        resp = requests.post(f"{addr}/admin/elect_leader/{election_id}")
        resp.raise_for_status()
        data = resp.json()
        return data["leader"]
    except:
        return None


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
        worker_addrs = args.workers
    elif args.worker_ports is not None:
        worker_addrs = [f"http://localhost:{port}" for port in args.worker_ports]
    else:
        worker_addrs = []

    addr_to_uid = Network.get_uids(worker_addrs)
    workers = {uid: addr for addr, uid in addr_to_uid.items()}

    mtx = threading.RLock()
    last_probed = None
    leader = None
    election_id = 0

    def elect_leader():
        async def elect_leader_async():  # NOTE: Kuba please take a look - looks ugly to me
            nonlocal election_id
            leader_responses = await asyncio.gather(
                *[
                    asyncio.to_thread(
                        get_election_result, uid, addr, election_id, logger
                    )
                    for uid, addr in workers.items()
                ]
            )
            election_id += 1
            leaders = set(resp for resp in leader_responses if resp is not None)
            with mtx:
                nonlocal leader
                if len(leaders) != 1:
                    logger.error(f"failed to elect leaders, got {leaders}")
                else:
                    leader = leaders.pop()
                    logger.info(
                        f"elected node[{addr_to_uid[leader]}] ({leader}) to be the leader"
                    )
                if args.leader_update_cb is not None:
                    requests.put(
                        args.leader_update_cb,
                        json={"leader": leader},
                    )

        asyncio.run(elect_leader_async())

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
