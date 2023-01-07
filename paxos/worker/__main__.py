import argparse
import asyncio
import http
import logging
import signal
import threading
from pathlib import Path
from threading import Thread

from flask import Flask, jsonify, request
from marshmallow import Schema, ValidationError, fields

from paxos.logic.communication import Network
from paxos.logic.kvstore import KeyValueStore

from .ledger import FileLedger, LedgerError


class Worker:
    def __init__(self):
        pass

    def parse_args(self):
        p = argparse.ArgumentParser()

        p.add_argument("--flask-port", type=int, required=True)
        p.add_argument("--comm-port", type=int, required=True)
        p.add_argument("--ledger-file", required=True)
        p.add_argument("--comm-net", nargs="*")
        p.add_argument("--paxos-dir", required=True)
        p.add_argument("-v", "--verbose", action="store_true")

        return p.parse_args()

    def setup_logging(self):
        logging.getLogger("werkzeug").setLevel(logging.WARN)
        level = logging.INFO if self.args.verbose else logging.WARN
        logging.basicConfig(level=level)

    def make_flask_app(self):
        app = Flask(__name__)

        @app.errorhandler(LedgerError)
        def on_ledger_error(error: LedgerError):
            code = http.HTTPStatus.BAD_REQUEST
            resp = jsonify({"error": "LedgerError", "details": str(error)})
            return resp, code

        @app.errorhandler(ValidationError)
        def on_validation_error(error: ValidationError):
            code = http.HTTPStatus.BAD_REQUEST
            resp = jsonify({"error": "ValidationError", "details": error.messages})
            return resp, code

        @app.post("/account")
        def open_account():
            uid = self.ledger.open_acct()
            return {"uid": uid}

        @app.get("/account/<int:uid>")
        def account(uid):
            acct = self.ledger.account(uid)
            return {"uid": acct.uid, "funds": acct.funds}

        class DepositSchema(Schema):
            uid = fields.Int()
            amount = fields.Decimal()

        @app.post("/deposit")
        def deposit():
            data = DepositSchema().load(request.json)
            self.ledger.deposit(data["uid"], data["amount"])
            return {}

        class WithdrawalSchema(Schema):
            uid = fields.Int()
            amount = fields.Decimal()

        @app.post("/withdrawal")
        def withdrawal():
            data = WithdrawalSchema().load(request.json)
            self.ledger.withdraw(data["uid"], data["amount"])
            return {}

        class TransferSchema(Schema):
            from_uid = fields.Int()
            to_uid = fields.Int()
            amount = fields.Decimal()

        @app.post("/transfer")
        def transfer_funds():
            data = TransferSchema().load(request.json)
            self.ledger.transfer(
                from_uid=data["from_uid"],
                to_uid=data["to_uid"],
                amount=data["amount"],
            )
            return {}

        @app.get("/admin/healthcheck")
        def healthcheck():
            return {}

        @app.put("/admin/paxos")
        async def set_paxos():
            for key, value in request.args.items():
                await self.kv_store.set(key, value)
            return {}

        @app.get("/admin/paxos/<key>")
        async def get_paxos(key):
            return await self.kv_store[key]

        @app.post("/admin/elect_leader")
        async def elect_leader():
            # TODO Implement this part (Basic Paxos)
            leader = f"http://{request.host}"
            return {"leader": leader}

        return app

    def make_comm_server(self):
        addr = f"localhost:{self.args.comm_port}"
        net = Network.from_addresses(self.args.comm_net, addr)
        save_path = Path(self.args.paxos_dir) / f"{net.me.id}.pkl"
        paxos_log = logging.getLogger(f"paxos-{net.me.id}").info
        self.kv_store = KeyValueStore(net, save_path=save_path, log=paxos_log)
        return self.kv_store.UDP_KV_Server()

    def main(self):
        self.args = self.parse_args()
        self.setup_logging()
        self.ledger = FileLedger(fpath=Path(self.args.ledger_file))

        flask_app = self.make_flask_app()
        flask_thr = Thread(
            target=lambda: flask_app.run(
                use_reloader=False,
                debug=False,
                port=self.args.flask_port,
            ),
            daemon=True,
        )

        def comm_thr_fn():
            with self.make_comm_server() as srv:
                srv.serve_forever()

        comm_thr = Thread(target=comm_thr_fn, daemon=True)

        flask_thr.start()
        comm_thr.start()

        finishing = threading.Event()

        def handler(signo, frame):
            finishing.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, handler)

        finishing.wait()


if __name__ == "__main__":
    worker = Worker()
    worker.main()
