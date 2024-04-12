#!/bin/bash

# from https://docs.docker.com/engine/swarm/stack-deploy/

# init docker swarm
docker swarm init

# should be disabled during development because it triggers an image rebuild
if false; then
rm -rf dbi/build
rm -rf cell-profile/build

rm -rf web-frontend/dbi_and_cell-profile
mkdir web-frontend/dbi_and_cell-profile
cp -r dbi web-frontend/dbi_and_cell-profile/dbi
cp -r cell-profile web-frontend/dbi_and_cell-profile/cell-profile

rm -rf script_workers/dbi_and_cell-profile
mkdir script_workers/dbi_and_cell-profile
cp -r dbi script_workers/dbi_and_cell-profile/dbi
cp -r cell-profile script_workers/dbi_and_cell-profile/cell-profile
fi

DOCKER_CACHE_FLAG="" # --no-cache"

# Define your local registry
REGISTRY_PORT=5000
REGISTRY=127.0.0.1:$REGISTRY_PORT

# start docker registry (as docker service itself)
docker-compose -f registry-compose.yaml build
docker-compose -f registry-compose.yaml up -d

# list running services (which should include the registry)
docker service ls 

docker pull localstack/localstack:s3-latest
docker tag localstack/localstack:s3-latest $REGISTRY/s3:latest
docker push $REGISTRY/s3:latest

docker pull mariadb:11.3
docker tag mariadb:11.3 $REGISTRY/db:latest
docker push $REGISTRY/db:latest

WORKDIR=$(pwd)

cd rabbitmq
docker buildx build $DOCKER_CACHE_FLAG -f rabbitmq.Dockerfile -t $REGISTRY/taskq:latest .
docker push $REGISTRY/taskq:latest
cd $WORKDIR

cd web-frontend
docker buildx build $DOCKER_CACHE_FLAG -f webserver.Dockerfile -t $REGISTRY/webfrontend:latest .
docker push $REGISTRY/webfrontend:latest
cd $WORKDIR

cd script_workers
docker buildx build $DOCKER_CACHE_FLAG -f cpmapper.Dockerfile -t $REGISTRY/cpmapper:latest .
docker push $REGISTRY/cpmapper:latest
cd $WORKDIR

cd script_workers
docker buildx build $DOCKER_CACHE_FLAG -f cpreducer.Dockerfile -t $REGISTRY/cpreducer:latest .
docker push $REGISTRY/cpreducer:latest
cd $WORKDIR

docker stack deploy --compose-file docker-compose.yaml myproject

# docker swarm stuff

# investigate a node
docker node inspect self --pretty # replace self with node ip to inspect another node

# scale service
if false; then
docker service scale myproject_cpmapper=6
fi
