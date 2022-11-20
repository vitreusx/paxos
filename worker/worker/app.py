from flask import Flask, request, jsonify
from .ledger import FileLedger, LedgerError
from .config import Config
from pathlib import Path
import http
from marshmallow import Schema, fields, ValidationError
import os
import signal


def create_app():
    app = Flask(__name__)
    config = Config.from_env()

    ledger_path = Path(config.LEDGER_PATH)
    ledger = FileLedger(fpath=ledger_path)

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

    @app.put("/admin/kill")
    def kill():
        func = request.environ.get("werkzeug.server.shutdown")
        if func is None:
            os.kill(os.getpid(), signal.SIGINT)
        else:
            func()
        return {}

    return app
