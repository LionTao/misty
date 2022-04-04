#!/bin/bash

set -euo pipefail

dapr init
sed -i "s/\"1\"/\"0\"/g" /home/liontao/.dapr/config.yaml

#dapr run --app-id assemble --app-port 3000 -- uvicorn --port 3000 --no-access-log assemble.main:app > assemble.log 2>&1 & echo $! > pid
#dapr run --app-id index --app-port 3001 -- uvicorn --port 3001 --no-access-log index.main:app > index.log 2>&1 & echo $! >> pid
#dapr run --app-id index-meta --app-port 3002 -- uvicorn --port 3002 --no-access-log index_meta.main:app > index-meta.log 2>&1 & echo $! >> pid

dapr run --app-id assemble --app-port 3000 -- hypercorn   assemble.main:app --bind 0.0.0.0:3000  > assemble.log 2>&1 & echo $! > pid
dapr run --app-id index --app-port 3001 -- hypercorn   index.main:app --bind 0.0.0.0:3001  > index.log 2>&1 & echo $! >> pid
dapr run --app-id index-meta --app-port 3002 -- hypercorn   index_meta.main:app --bind 0.0.0.0:3002  > index-meta.log 2>&1 & echo $! >> pid
dapr run --app-id compute --app-port 3003 -- hypercorn   compute.main:app --bind 0.0.0.0:3003  > compute.log 2>&1 & echo $! >> pid
dapr run --app-id agent --app-port 3004 -- hypercorn   agent.main:app --bind 0.0.0.0:3004  > agent.log 2>&1 & echo $! >> pid


sleep 5
dapr run --app-id ingress python3 ingress/main.py