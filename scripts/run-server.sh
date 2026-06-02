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
    docker compose logs --tail=200
    exit 1
  fi
done
