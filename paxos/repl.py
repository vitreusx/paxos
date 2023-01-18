import argparse
import http
import shlex
from urllib.parse import urljoin

import prompt_toolkit as pt
import requests
from prompt_toolkit.completion import WordCompleter


class UserExit(Exception):
    pass


def main():
    p = argparse.ArgumentParser()
    p.add_argument("-u", "--url")
    p.add_argument("-p", "--port")
    p.add_argument("-e", "--exec")

    args = p.parse_args()
    if args.url is not None:
        url = args.url
    else:
        assert args.port is not None
        url = f"http://localhost:{args.port}"

    opt_p = argparse.ArgumentParser()
    opt_sp = opt_p.add_subparsers(dest="endpoint")

    help_p = opt_sp.add_parser("help")
    help_p.add_argument(
        "command",
        default=None,
        choices=["account", "withdraw", "deposit", "transfer"],
    )

    account_p = opt_sp.add_parser("account")
    acc_mtx = account_p.add_mutually_exclusive_group()
    acc_mtx.add_argument("--status", type=int)
    acc_mtx.add_argument("--create", action="store_true")

    withdraw_p = opt_sp.add_parser("withdraw")
    withdraw_p.add_argument("account_id", type=int)
    withdraw_p.add_argument("amount", type=str)

    deposit_p = opt_sp.add_parser("deposit")
    deposit_p.add_argument("account_id", type=int)
    deposit_p.add_argument("amount", type=str)

    transfer_p = opt_sp.add_parser("transfer")
    transfer_p.add_argument("from_", type=int, metavar="from")
    transfer_p.add_argument("to", type=int)
    transfer_p.add_argument("amount", type=str)

    quit_p = opt_sp.add_parser("quit")

    def on_prompt(text):
        try:
            args = opt_p.parse_args(shlex.split(text))
        except SystemExit as e:
            return

        try:
            resp = None
            if args.endpoint == "help":
                if args.command is not None:
                    p = {
                        "account": account_p,
                        "withdraw": withdraw_p,
                        "deposit": deposit_p,
                        "transfer": transfer_p,
                        "quit": quit_p,
                    }[args.command]
                    p.print_help()
                else:
                    opt_p.print_help()
            elif args.endpoint == "account":
                if args.create:
                    req_url = urljoin(url, "/account")
                    resp = requests.post(req_url)
                    resp.raise_for_status()
                    data = resp.json()
                    print(f"Created account #{data['uid']}")
                elif args.status is not None:
                    req_url = urljoin(url, f"/account/{args.status}")
                    resp = requests.get(req_url)
                    resp.raise_for_status()
                    data = resp.json()
                    print(f"Account #{data['uid']}: ${data['funds']}")
                else:
                    account_p.print_help()
            elif args.endpoint == "withdraw":
                req_url = urljoin(url, "/withdraw")
                payload = {"uid": args.account_id, "amount": args.amount}
                resp = requests.post(req_url, json=payload)
                resp.raise_for_status()
            elif args.endpoint == "deposit":
                req_url = urljoin(url, "/deposit")
                payload = {"uid": args.account_id, "amount": args.amount}
                resp = requests.post(req_url, json=payload)
                resp.raise_for_status()
            elif args.endpoint == "transfer":
                req_url = urljoin(url, "/transfer")
                payload = {
                    "from_uid": args.from_,
                    "to_uid": args.to,
                    "amount": args.amount,
                }
                resp = requests.post(req_url, json=payload)
                resp.raise_for_status()
            elif args.endpoint == "quit":
                raise UserExit
        except (requests.HTTPError, requests.ConnectionError) as e:
            if e.errno == http.HTTPStatus.BAD_REQUEST.value:
                error_data = resp.json()
                print(f"[{error_data['error']}] {error_data['details']}")
            else:
                print(e)

    if args.exec:
        on_prompt(args.exec)
    else:
        completer = WordCompleter(["help"])
        sess = pt.PromptSession("> ", completer=completer)

        while True:
            try:
                text = sess.prompt()
                on_prompt(text)
            except (KeyboardInterrupt, EOFError, UserExit):
                break


if __name__ == "__main__":
    main()
