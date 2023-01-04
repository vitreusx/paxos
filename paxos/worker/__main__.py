import argparse
import http
import logging
from pathlib import Path

from flask import Flask, jsonify, request
from marshmallow import Schema, ValidationError, fields

from .ledger import FileLedger, LedgerError


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--ledger-file", required=True)
    p.add_argument("--other-nodes", nargs="*")
    p.add_argument("-v", "--verbose", action="store_true")

    args = p.parse_args()

    other_nodes = args.other_nodes

    app = Flask(__name__)

    logging.getLogger("werkzeug").setLevel(logging.WARN)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARN)

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

    @app.post("/admin/elect_leader")
    def elect_leader():
        # TODO Implement this part (Basic Paxos)
        return {"leader": f"http://{request.host}"}

    app.run(debug=False, port=args.port)


if __name__ == "__main__":
    main()
