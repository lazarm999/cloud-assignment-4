#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $(basename "$0") <bucketNo>"
  exit 1
fi

# Monitor the IO read activity of a command
traceIO() {
  touch "$1"
  "${@:2}" > "$1" &
}

# Spawn the coordinator process
cmake-build-debug/coordinator 4242 "$1" > output.txt &

# Spawn some workers
for i in {1..4}; do
  traceIO "trace$i.txt" \
    cmake-build-debug/worker "localhost" "4242" &
done

# And wait for completion
time wait

# Then check how much work each worker did
for i in {1..4}; do
  # We need to download 100 files. While we might get some skew, each of our
  # four workers should do about 20 of them. But definitely more than 10.
  load=$(grep "Task 1" "trace$i.txt" | cut -d':' -f2)
  if [ "$load" -lt 10 ]; then
    echo "Workload for the first was not distributed evenly!" >&2
    echo "Worker $i was slacking." >&2
    for j in {0..4}; do rm "trace$j.txt"; done
    exit 1
  fi
  load=$(grep "Task 2" "trace$i.txt" | cut -d':' -f2)
  if [ "$load" -lt $(($1/8)) ]; then
    echo "Workload for the second task was not distributed evenly!" >&2
    echo "Worker $i was slacking." >&2
    for j in {0..4}; do rm "trace$j.txt"; done
    exit 1
  fi
done

echo "Workload was distributed evenly!"

for i in {1..4}; do rm "trace$i.txt"; done