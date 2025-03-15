#FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build-env
FROM kahuna-base AS build-env

RUN rm -fr /src && mkdir -p /src
#COPY Kommander /src/Kommander/
COPY Kahuna.Shared /src/Kahuna.Shared/
COPY Kahuna.Server /src/Kahuna.Server/

# build the dotnet program
WORKDIR /

RUN cd /src/Kahuna.Server/ && dotnet publish -c release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app

# expose the ports
EXPOSE 2070
EXPOSE 2071

# copy the built program
COPY --from=build-env /app .
COPY certs/development-certificate.pfx /app/certificate.pfx

# install sqlite to debug
# RUN apt update && apt upgrade && apt-get -y install sqlite3

ARG KAHUNA_RAFT_NODEID
ARG KAHUNA_RAFT_HOST
ARG KAHUNA_RAFT_PORT
ARG KAHUNA_HTTP_PORTS
ARG KAHUNA_HTTPS_PORTS
ARG KAHUNA_INITIAL_CLUSTER

ENV KAHUNA_RAFT_NODEID="$KAHUNA_RAFT_NODEID"
ENV KAHUNA_RAFT_HOST="$KAHUNA_RAFT_HOST"
ENV KAHUNA_RAFT_PORT="$KAHUNA_RAFT_PORT"
ENV KAHUNA_HTTP_PORTS="$KAHUNA_HTTP_PORTS"
ENV KAHUNA_HTTPS_PORTS="$KAHUNA_HTTPS_PORTS"
ENV KAHUNA_INITIAL_CLUSTER="$KAHUNA_INITIAL_CLUSTER"

COPY --chmod=755 <<EOT /app/entrypoint.sh
#!/usr/bin/env bash

mkdir -p /storage/data/v3
mkdir -p /storage/wal/v3
mkdir -p /storage/data/v1
mkdir -p /storage/wal/v1

echo "kahuna --raft-nodeid $KAHUNA_RAFT_NODEID --raft-host $KAHUNA_RAFT_HOST --raft-port $KAHUNA_RAFT_PORT --http-ports $KAHUNA_HTTP_PORTS --https-ports $KAHUNA_HTTPS_PORTS --https-certificate /app/certificate.pfx --initial-cluster $KAHUNA_INITIAL_CLUSTER --storage rocksdb --storage-path /storage/data --storage-revision v1 --wal-path /storage/wal --wal-revision v3"

dotnet /app/Kahuna.Server.dll --raft-nodeid $KAHUNA_RAFT_NODEID --raft-host $KAHUNA_RAFT_HOST --raft-port $KAHUNA_RAFT_PORT --http-ports $KAHUNA_HTTP_PORTS --https-ports $KAHUNA_HTTPS_PORTS --https-certificate /app/certificate.pfx --initial-cluster $KAHUNA_INITIAL_CLUSTER --storage rocksdb --storage-path /storage/data --storage-revision v1 --wal-path /storage/wal --wal-revision v3
EOT

# when starting the container, run dotnet with the built dll
ENTRYPOINT [ "/app/entrypoint.sh" ]

# Swap entrypoints if the container is exploding and you want to keep it alive indefinitely so you can go look into it.
#ENTRYPOINT ["tail", "-f", "/dev/null"]

