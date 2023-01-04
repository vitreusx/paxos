#!/usr/bin/sh
python3 -m paxos.with_leader \
  --kill-every 1.0 0.2 \
  --restart-after 2.0 \
  --ledger-file ledger.yml \
  --num-workers 5 \
  --probe-period 0.1 \
  --prober-port 8000 \
  --gateway-port 8001
