import argparse
import http
import logging
import signal
import threading
from pathlib import Path
from threading import Thread

from flask import Flask, jsonify, request
from marshmallow import Schema, ValidationError, fields

from paxos.ledger.base import LedgerError
from paxos.ledger.file import FileLedger
from paxos.logic.communication import Network
from paxos.logic.multi import MultiPaxos
from paxos.logic.types import PaxosVar


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
        p.add_argument("--generator", type=str, choices=["incremental", "time_aware"])
        p.add_argument("-v", "--verbose", action="store_true")

        return p.parse_args()

    def setup_logging(self):
        logging.getLogger("werkzeug").setLevel(logging.WARN)

    def setup_flask(self):
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

        @app.post("/withdraw")
        def withdraw():
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

        @app.post("/admin/elect_leader")
        async def elect_leader():
            proposal = f"http://{request.host}"
            await self.leader.set(proposal)
            return {"leader": proposal}

        self.flask_thr = Thread(
            target=lambda: app.run(
                use_reloader=False,
                debug=False,
                port=self.args.flask_port,
            ),
            daemon=True,
        )

        self.flask_thr.start()

    def setup_paxos(self):
        addr = f"localhost:{self.args.comm_port}"
        net = Network.from_addresses(self.args.comm_net, addr)
        save_path = Path(self.args.paxos_dir) / f"node-{net.me.id}.pkl"
        self.paxos = MultiPaxos(net, save_path, self.args.generator)
        self.leader = PaxosVar(self.paxos, "leader", None)

        def comm_fn():
            with self.paxos.UDP_Server() as srv:
                self.paxos_srv = srv
                self.paxos_srv.serve_forever()

        self.comm_thr = Thread(
            target=comm_fn,
            daemon=False,
        )

        self.comm_thr.start()

    def main(self):
        self.args = self.parse_args()
        self.setup_logging()
        self.ledger = FileLedger(fpath=Path(self.args.ledger_file))

        self.finishing = threading.Event()
        self.setup_paxos()
        self.setup_flask()

        def handler(signo, frame):
            self.finishing.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, handler)

        self.finishing.wait()

        self.paxos_srv.shutdown()
        self.comm_thr.join()


if __name__ == "__main__":
    worker = Worker()
    worker.main()
