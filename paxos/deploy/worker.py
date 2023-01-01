from .killer import AbstractWorker
from pathlib import Path
from typing import List, Union
import subprocess
from subprocess import DEVNULL

class PaxosWorker(AbstractWorker):
    def __init__(self, flask_port: int, comm_port: int, ledger_file: Union[str, Path], comm_net: List[str], verbose: bool):
        super().__init__()
        self.flask_port = flask_port
        self.comm_port = comm_port
        self.ledger_file = Path(ledger_file).absolute()
        self.comm_net = comm_net
        self.verbose = verbose
        self._proc = None
    
    def kill(self):
        if self.is_alive() and self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
    
    def respawn(self):
        if not self.is_alive():
            args = ["python3", "-m", "paxos.worker"]
            args.extend(["--flask-port", str(self.flask_port)])
            args.extend(["--comm-port", str(self.comm_port)])
            args.extend(["--ledger-file", str(self.ledger_file)])
            args.extend(["--comm-net", *self.comm_net])
            if self.verbose:
                args.extend(["-v"])
            
            self._proc = subprocess.Popen(args, stdout=DEVNULL)
    
    def is_alive(self):
        if self._proc is None:
            return False
        
        if self._proc.poll() is None:
            return True
        else:
            return False