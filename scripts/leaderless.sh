#!/usr/bin/sh
python3 -m paxos.deploy.leaderless \
  --ledger-file ledger.yml \
  --num-workers 5 \
  --gateway-port 8001 \
  --kill-every 1.0 0.2 \
  --restart-after 2.0 \
  --killer-port 8002 \
  --killer-type random \
  --generator incremental \
  --verbose \
  $@ 2>&1 | tee log.txt

