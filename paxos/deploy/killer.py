import logging
import threading
import time
from typing import List, Optional

import numpy as np
from scipy.stats import rv_continuous

from paxos.deploy.worker import AbstractWorker


class Killer(threading.Thread):
    def __init__(
        self,
        workers: List[AbstractWorker],
        finishing: threading.Event,
        kill_every: rv_continuous,
        restart_after: Optional[rv_continuous] = None,
    ):
        super().__init__()
        self.workers = workers
        self.kill_every = kill_every
        self.restart_after = restart_after
        self.finishing = finishing
        self.gen = np.random.default_rng()
        self.log = logging.getLogger("killer").info

    def run(self):
        timers_mtx = threading.Lock()
        timers = {}
        timer_id = 0
        self.any_alive_cv = threading.Condition()

        while not self.finishing.is_set():
            with self.any_alive_cv:
                self.any_alive_cv.wait_for(
                    lambda: any(w.is_alive() for w in self.workers)
                    or self.finishing.is_set()
                )

                if self.finishing.is_set():
                    break

                alive_idxes = np.array(
                    [idx for idx, w in enumerate(self.workers) if w.is_alive()]
                )
                kill_idx = self.gen.choice(alive_idxes)

            worker = self.workers[kill_idx]
            self.log(f"killing {worker}")
            worker.kill()

            if self.restart_after is not None:

                def restart_fn(kill_idx_, timer_id_):
                    worker = self.workers[kill_idx_]
                    with self.any_alive_cv:
                        worker.respawn()
                        self.log(f"revived {worker}")
                        self.any_alive_cv.notify()

                    with timers_mtx:
                        if timer_id_ in timers:
                            del timers[timer_id_]

                delay = self.restart_after.rvs(random_state=self.gen)
                with timers_mtx:
                    restart_timer = threading.Timer(
                        delay,
                        restart_fn,
                        (
                            kill_idx,
                            timer_id,
                        ),
                    )
                    timers[timer_id] = restart_timer
                    restart_timer.start()
                    timer_id += 1

            delay = self.kill_every.rvs(random_state=self.gen)
            time.sleep(delay)

        with timers_mtx:
            for timer in timers.values():
                timer.cancel()
                timer.join()

    def join(self, timeout: Optional[float] = None):
        with self.any_alive_cv:
            self.any_alive_cv.notify()
        super().join(timeout=timeout)
