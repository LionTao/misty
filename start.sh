#!/bin/bash

set -euo pipefail

ulimit -n 64000
ulimit -u 64000
ulimit -f unlimited
ulimit -t unlimited
ulimit -m unlimited
ulimit -v unlimited

dapr init --from-dir /home/jctao20204227021/Downloads/daprbundle
docker run -d --name jaeger \
  -e COLLECTOR_ZIPKIN_HOST_PORT=:9412 \
  -p 16686:16686 \
  -p 9412:9412 \
  jaegertracing/all-in-one:1.22
sed -i "s@http://localhost:9411/api/v2/spans@http://localhost:9412/api/v2/spans@g" /home/jctao20204227021/.dapr/config.yaml
docker run --name "dapr_zipkin" --restart always -d -p 9411:9411 openzipkin/zipkin
docker run --name "dapr_redis" --restart always -d -p 6379:6379 redislabs/rejson

# docker run --name prom -p 8888:9090 -d  -v /home/liontao/work/pdbst-rtree/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus --config.file=/etc/prometheus/prometheus.yml --web.listen-address=:9090

# docker run --name grafana -d -p 4000:3000 grafana/grafana-oss:8.4.5-ubuntu

# 3000 is for grafana
# dapr run --app-id assemble --app-port 3000 -- hypercorn --bind 0.0.0.0:3000 assemble.main:app >assemble.log 2>&1 &
# echo $! >pid
# dapr run --app-id index --app-port 3001 -- hypercorn --bind 0.0.0.0:3001 index.main:app >index.log 2>&1 &
# echo $! >>pid
# dapr run --app-id index-meta --app-port 3002 -- hypercorn --bind 0.0.0.0:3002 index_meta.main:app >index-meta.log 2>&1 &
# echo $! >>pid
# dapr run --app-id compute --app-port 3003 -- hypercorn --bind 0.0.0.0:3003 compute.main:app >compute.log 2>&1 &
# echo $! >>pid
# dapr run --app-id agent --app-port 3004 -- hypercorn --bind 0.0.0.0:3004 agent.main:app >agent.log 2>&1 &
# echo $! >>pid

touch pid
for i in {1..20}; do
  let p=3000+$i
  dapr run --app-id assemble --app-port "$p" -- hypercorn --bind "0.0.0.0:$p" assemble.main:app >"logs/assemble$i.log" 2>&1 &
  echo $! >>pid
done

for i in {1..30}; do
  let p=3100+$i
  dapr run --app-id index --app-port "$p" -- hypercorn --bind "0.0.0.0:$p" index.main:app >"logs/index$i.log" 2>&1 &
  echo $! >>pid
done

dapr run --app-id index-meta --app-port 3301 -- hypercorn --bind "0.0.0.0:3301" index_meta.main:app >"logs/index-meta.log" 2>&1 &
echo $! >>pid

for i in {1..5}; do
  let p=3200+$i
  dapr run --app-id compute --app-port "$p" -- hypercorn --bind "0.0.0.0:$p" compute.main:app >"logs/compute$i.log" 2>&1 &
  echo $! >>pid
done

dapr run --app-id agent --app-port 3302 -- hypercorn --bind 0.0.0.0:3302 agent.main:app >logs/agent.log 2>&1 &
echo $! >>pid

sleep 10
dapr run --app-id ingress python3 ingress/main.py
