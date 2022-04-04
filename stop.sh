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
