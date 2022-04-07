#!/bin/bash

set -euo pipefail

ulimit -n 65535

dapr init
docker run -d --name jaeger \
  -e COLLECTOR_ZIPKIN_HOST_PORT=:9412 \
  -p 16686:16686 \
  -p 9412:9412 \
  jaegertracing/all-in-one:1.22
sed -i "s@http://localhost:9411/api/v2/spans@http://localhost:9412/api/v2/spans@g" /home/liontao/.dapr/config.yaml
echo ""

docker run --name prom -p 8888:9090 -d  -v /home/liontao/work/pdbst-rtree/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus --config.file=/etc/prometheus/prometheus.yml --web.listen-address=:9090

docker run --name grafana -d -p 4000:3000 grafana/grafana-oss:8.4.5-ubuntu

# 3000 is for grafana
dapr run --app-id assemble -M 9090 --app-port 3000 -- hypercorn   assemble.main:app --bind 0.0.0.0:3000  > assemble.log 2>&1 & echo $! > pid
dapr run --app-id index -M 9091 --app-port 3001 -- hypercorn   index.main:app --bind 0.0.0.0:3001  > index.log 2>&1 & echo $! >> pid
dapr run --app-id index-meta -M 9092 --app-port 3002 -- hypercorn   index_meta.main:app --bind 0.0.0.0:3002  > index-meta.log 2>&1 & echo $! >> pid
dapr run --app-id compute -M 9093 --app-port 3003 -- hypercorn   compute.main:app --bind 0.0.0.0:3003  > compute.log 2>&1 & echo $! >> pid
dapr run --app-id agent -M 9094 --app-port 3004 -- hypercorn   agent.main:app --bind 0.0.0.0:3004  > agent.log 2>&1 & echo $! >> pid


sleep 5
dapr run --app-id ingress python3 ingress/main.py