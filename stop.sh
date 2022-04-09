#!/bin/bash

if [ -f "pid" ]; then
    while read LINE
    do
      kill -15 $LINE
    done < ./pid
fi
#rm ./*.out
rm ./pid
dapr uninstall --all

docker stop jaeger
docker rm jaeger

# docker stop prom
# docker rm prom

# docker stop grafana
# docker rm grafana
