# cmake-build-debug/coordinator "https://db.in.tum.de/teaching/ws2223/clouddataprocessing/data/filelist.csv" 4242 &

# # Spawn some workers
# for _ in {1..4}; do
#   cmake-build-debug/worker "localhost" "4242" &
# done

# # And wait for completion
# time wait
#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $(basename "$0") <bucketNo>"
  exit 1
fi

# Spawn the coordinator process
cmake-build-debug/coordinator 4242 "$1" &

# Spawn some workers
for _ in {1..4}; do
  cmake-build-debug/worker "localhost" "4242" &
done

# And wait for completion
time wait