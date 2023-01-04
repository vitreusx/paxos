from test.on_references.test_messenger import DebugReferenceMessenger
from typing import Dict

from paxos.logic.core import Acceptor, Learner, Proposer
from paxos.logic.helper import BasicIDGenerator


def main():
    proposers_uids = [1, 2, 3]
    acceptors_uids = [uid + 3 for uid in proposers_uids]
    learners_uids = [uid + 3 for uid in acceptors_uids]
    proposers = {
        str(uid): Proposer(
            str(uid),
            2,
            BasicIDGenerator(uid, 3),
            DebugReferenceMessenger(
                uid,
            ),
        )
        for uid in proposers_uids
    }
    acceptors = {
        str(uid): Acceptor(
            str(uid),
            2,
            DebugReferenceMessenger(
                uid,
            ),
        )
        for uid in acceptors_uids
    }
    learners = {
        str(uid): Learner(
            str(uid),
            2,
            DebugReferenceMessenger(
                uid,
            ),
        )
        for uid in learners_uids
    }

    for node in [*acceptors.values(), *learners.values(), *proposers.values()]:
        node.messenger.init_nodes(proposers, acceptors, learners)

    print(f"{proposers=}, {acceptors=}, {learners=}")
    proposers["1"].propose("qupa")

    for node in [*acceptors.values(), *learners.values(), *proposers.values()]:
        node.reset()
    proposers["2"].propose("gunwo")


if __name__ == "__main__":
    main()
