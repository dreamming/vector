#!/bin/bash

set -o errexit;
set -o nounset;
set -o pipefail;

#if [ ! -f sample.log ]; then
#  echo "generating sample.log"
#  docker run -it --rm mingrammer/flog -b $((10 * 1024 * 1024)) > sample.log
#fi

cargo build --release

echo "input: $(wc -l < sample.log) lines"

./target/release/myvector & docker run -it --rm --network=host alpine /bin/sh -c "nc localhost 8081"
#time cat sample.log |
wait


