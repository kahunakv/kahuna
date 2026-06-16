#!/bin/bash
dotnet publish Kahuna.Server/Kahuna.Server.csproj -o /tmp/kahuna

cp certs/development-certificate.pfx /tmp/kahuna/certificate.pfx

cd /tmp/kahuna

# Create only the parent dirs; RocksDB creates each <path>/<revision> dir itself
# and treats an existing revision dir as a pre-existing (versioned) store.
mkdir -p /tmp/kh1/wal
mkdir -p /tmp/kh1/data
mkdir -p /tmp/kh2/wal
mkdir -p /tmp/kh2/data
mkdir -p /tmp/kh3/wal
mkdir -p /tmp/kh3/data

pids=()

dotnet Kahuna.Server.dll --raft-nodename kahuna3 --raft-nodeid 3 --raft-host 192.168.10.40 --raft-port 8086 --http-ports 8085 --https-ports 8086 --https-certificate /tmp/kahuna/certificate.pfx --initial-cluster 192.168.10.5:8082 192.168.10.42:8084 --storage rocksdb --storage-path /tmp/kh3/data --storage-revision v1 --wal-path /tmp/kh3/wal --wal-revision v1
pids[${i}]=$!

# wait for all pids
for pid in ${pids[*]}; do
    wait $pid
done
