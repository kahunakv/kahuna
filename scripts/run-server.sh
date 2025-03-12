#!/bin/bash
cd ..
docker build -f DockerfileBase  -t kahuna-base --progress=plain .
docker compose up --build -d