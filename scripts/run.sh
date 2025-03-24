#!/bin/bash
dotnet publish Kahuna.Server/Kahuna.Server.csproj -o /tmp/kahuna

cd /tmp/kahuna
cp certs/development-certificate.pfx /tmp/kahuna/certificate.pfx

mkdir -p /tmp/kh1/wal/v1
mkdir -p /tmp/kh1/data/v1
mkdir -p /tmp/kh2/wal/v1
mkdir -p /tmp/kh2/data/v1
mkdir -p /tmp/kh3/wal/v1
mkdir -p /tmp/kh3/data/v1

pids=()

dotnet Kahuna.Server.dll --raft-nodeid kahuna1 --raft-host 127.0.0.1 --raft-port 8082 --http-ports 8081 --https-ports 8082 --https-certificate /tmp/kahuna/certificate.pfx --initial-cluster 127.0.0.1:8084 127.0.0.1:8086 --storage rocksdb --storage-path /tmp/kh1/data --storage-revision v1 --wal-path /tmp/kh1/wal --wal-revision v1 &
pids[${i}]=$!

dotnet Kahuna.Server.dll --raft-nodeid kahuna2 --raft-host 127.0.0.1 --raft-port 8084 --http-ports 8083 --https-ports 8084 --https-certificate /tmp/kahuna/certificate.pfx --initial-cluster 127.0.0.1:8082 127.0.0.1:8086 --storage rocksdb --storage-path /tmp/kh2/data --storage-revision v1 --wal-path /tmp/kh2/wal --wal-revision v1 &
pids[${i}]=$!

dotnet Kahuna.Server.dll --raft-nodeid kahuna3 --raft-host 127.0.0.1 --raft-port 8086 --http-ports 8085 --https-ports 8086 --https-certificate /tmp/kahuna/certificate.pfx --initial-cluster 127.0.0.1:8082 127.0.0.1:8084 --storage rocksdb --storage-path /tmp/kh3/data --storage-revision v1 --wal-path /tmp/kh3/wal --wal-revision v1 &
pids[${i}]=$!

# wait for all pids
for pid in ${pids[*]}; do
    wait $pid
done
