#!/usr/bin/sh

python3 -m paxos.deploy.leaderless \
  --kill-every 1.0 0.2 \
  --restart-after 2.0 \
  --ledger-file ledger.yml \
  --num-workers 5 \
  --gateway-port 8001 \
  $@

# python3 -m paxos.deploy.leaderless \
#   --ledger-file ledger.yml \
#   --num-workers 5 \
#   --gateway-port 8001 \
#   $@
