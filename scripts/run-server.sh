#!/bin/bash
set -euo pipefail

cd ..

sudo apt-get install -y ca-certificates
sudo cp certs/development-certificate.crt /usr/local/share/ca-certificates
sudo update-ca-certificates

#docker build -f DockerfileBase  -t kahuna-base --progress=plain .
export COMPOSE_FILE=docker/local.yml
docker compose up --build -d

for port in 8082 8084 8086; do
  ready=0

  for attempt in {1..60}; do
    if curl -k -sS -o /dev/null --connect-timeout 2 "https://localhost:${port}/"; then
      ready=1
      break
    fi

    sleep 1
  done

  if [ "$ready" -ne 1 ]; then
    echo "Kahuna server did not become ready on port ${port}"
    docker compose ps
    docker compose logs --tail=500
    exit 1
  fi
done

# HTTP liveness above only proves each listener accepts connections; it does not prove that
# Raft has elected a leader for every partition. A request that lands on a follower before its
# partition leader is decided is answered with a retry signal (type 101 = MustRetry). Starting
# the client suite in that window causes spurious failures, so wait until each node returns a
# *decided* answer for a real lock request (leaders elected and follower->leader forwarding works).
for port in 8082 8084 8086; do
  converged=0

  for attempt in {1..60}; do
    body=$(curl -k -sS --connect-timeout 2 -X POST "https://localhost:${port}/v1/locks/try-lock" \
      -H "Content-Type: application/json" \
      -d '{"resource":"__cluster_ready_probe__","lockId":"cHJvYmU=","expiresMs":1000,"durability":0}' || true)

    # Any decided lock outcome (Locked/Busy/...) means the partition leader is up. Keep waiting
    # only while the node is still electing (MustRetry) or the request could not be served.
    case "$body" in
      *'"type":101'*|"") ;;
      *'"type":'*) converged=1; break ;;
    esac

    sleep 1
  done

  if [ "$converged" -ne 1 ]; then
    echo "Kahuna cluster did not elect leaders (still electing) on port ${port}"
    docker compose ps
    # Per-service tail so each node's election/replication warnings are attributable, plus a full
    # interleaved tail for cross-node ordering. Helps pin down why one node fails to converge.
    for svc in kahuna1 kahuna2 kahuna3; do
      echo "===== ${svc} logs (tail) ====="
      docker compose logs --no-color --tail=300 "${svc}"
    done
    echo "===== interleaved logs (tail) ====="
    docker compose logs --no-color --tail=500
    exit 1
  fi
done
