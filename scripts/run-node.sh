#!/bin/bash
dotnet publish Kahuna.Server/Kahuna.Server.csproj -o /tmp/kahuna

cp certs/development-certificate.pfx /tmp/kahuna/certificate.pfx

cd /tmp/kahuna

mkdir -p /tmp/kh1/wal/v1
mkdir -p /tmp/kh1/data/v1
mkdir -p /tmp/kh2/wal/v1
mkdir -p /tmp/kh2/data/v1
mkdir -p /tmp/kh3/wal/v1
mkdir -p /tmp/kh3/data/v1

pids=()

dotnet Kahuna.Server.dll --raft-nodeid kahuna3 --raft-host 192.168.10.40 --raft-port 8086 --http-ports 8085 --https-ports 8086 --https-certificate /tmp/kahuna/certificate.pfx --initial-cluster 192.168.10.5:8082 192.168.10.42:8084 --storage rocksdb --storage-path /tmp/kh3/data --storage-revision v1 --wal-path /tmp/kh3/wal --wal-revision v1
pids[${i}]=$!

# wait for all pids
for pid in ${pids[*]}; do
    wait $pid
done
