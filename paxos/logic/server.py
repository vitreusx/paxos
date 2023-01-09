from paxos.logic.communication import Communicator, Role
from paxos.logic.roles import Acceptor, Learner, Proposer


class PaxosServer:
    """A "Paxos server", i.e. a node with all the behaviors."""

    def __init__(self, comm: Communicator):
        self.comm = comm
        self.acceptor = Acceptor(self.comm)
        quorum_size = len(self.comm.all_of(Role.ACCEPTOR)) // 2 + 1
        self.proposer = Proposer(self.comm, quorum_size)
        self.learner = Learner(self.comm)
        self.behaviors = [self.proposer, self.acceptor, self.learner]

    @property
    def state(self):
        return {
            Role.ACCEPTOR: self.acceptor.state,
            Role.PROPOSER: self.proposer.state,
            Role.LEARNER: self.learner.state,
        }

    @state.setter
    def state(self, value):
        self.acceptor.state = value[Role.ACCEPTOR]
        self.proposer.state = value[Role.PROPOSER]
        self.learner.state = value[Role.LEARNER]
