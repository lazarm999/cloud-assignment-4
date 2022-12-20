#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $(basename "$0") <bucketNo>"
  exit 1
fi

# Spawn the coordinator process
cmake-build-debug/coordinator 4242 "$1" > output.txt & 

# Spawn some workers
for _ in {1..4}; do
  cmake-build-debug/worker "localhost" "4242" > /dev/null &
done

# And wait for completion
time wait

# evaluate if there's any difference between true ranking and program's output
DIFF=$(diff results.txt output.txt | wc -l | cut -d' ' -f1)
if [ "$DIFF" -eq 0 ]; then
  echo "Correct!"
else
  echo "Incorrect!"
fi