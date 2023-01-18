import logging
import threading

from flask import Flask

from paxos.deploy.worker import AbstractWorker


class InteractiveKiller(threading.Thread):
    def __init__(
        self,
        workers: dict[int, AbstractWorker],
        finishing: threading.Event,
        flask_port: int,
    ):
        super().__init__()
        self.workers = workers
        self.finishing = finishing
        self.log = logging.getLogger("interactive-killer").info
        self.flask_port = flask_port

    def setup_flask(self):
        self.any_alive_cv = threading.Condition()

        app = Flask(__name__)
        self.app = app

        @app.post("/kill/<int:uid>")
        def kill(uid: int):
            worker = self.workers[uid]
            if worker is None:
                self.log(f"Uknown worker uid {uid}")
                return

            self.log(f"killing {worker} of uid {uid}")
            worker.kill()

        @app.post("/respawn/<int:uid>")
        def revive(uid: int):
            worker = self.workers[uid]
            if worker is None:
                self.log(f"Uknown worker uid {uid}")
                return

            self.log(f"respawning {worker} of uid {uid}")
            worker.respawn()
            return {}

    def join(self, timeout: float | None = None):
        with self.any_alive_cv:
            self.any_alive_cv.notify()
        super().join(timeout=timeout)

    def run(self):
        self.setup_flask()
        self.app.run(port=self.flask_port)
