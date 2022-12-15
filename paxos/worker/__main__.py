import argparse
from pathlib import Path
from flask import Flask, request, jsonify
from .ledger import FileLedger, LedgerError
from pathlib import Path
import http
from marshmallow import Schema, fields, ValidationError
from dataclasses import dataclass
from pathlib import Path
import logging


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--ledger-file", required=True)

    args = p.parse_args()

    app = Flask(__name__)

    logging.getLogger("werkzeug").setLevel(logging.WARN)

    ledger = FileLedger(fpath=Path(args.ledger_file))

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
        uid = ledger.open_acct()
        return {"uid": uid}

    @app.get("/account/<int:uid>")
    def account(uid):
        acct = ledger.account(uid)
        return {"uid": acct.uid, "funds": acct.funds}

    class DepositSchema(Schema):
        uid = fields.Int()
        amount = fields.Decimal()

    @app.post("/deposit")
    def deposit():
        data = DepositSchema().load(request.json)
        ledger.deposit(data["uid"], data["amount"])
        return {}

    class WithdrawalSchema(Schema):
        uid = fields.Int()
        amount = fields.Decimal()

    @app.post("/withdrawal")
    def withdrawal():
        data = WithdrawalSchema().load(request.json)
        ledger.withdraw(data["uid"], data["amount"])
        return {}

    class TransferSchema(Schema):
        from_uid = fields.Int()
        to_uid = fields.Int()
        amount = fields.Decimal()

    @app.post("/transfer")
    def transfer_funds():
        data = TransferSchema().load(request.json)
        ledger.transfer(data["from_uid"], data["to_uid"], data["amount"])
        return {}

    @app.get("/admin/healthcheck")
    def healthcheck():
        return {}

    class ElectLeaderSchema(Schema):
        other_nodes = fields.List(fields.Str())

    @app.post("/admin/elect_leader")
    def elect_leader():
        data = ElectLeaderSchema().load(request.json)
        other_nodes: list[str] = data["other_nodes"]
        # TODO Implement this part (Basic Paxos)
        return {"leader": f"http://{request.host}"}

    app.run(debug=False, port=args.port)


if __name__ == "__main__":
    main()
