#!/usr/bin/env bash

set -euxo pipefail

skipgateway=${1:-0}
if ! [[ "$skipgateway" =~ ^[0-9]+$ ]]; then
   echo "Integers only as first argument"
   exit 1
fi

# Retrieve the script directory.
SCRIPT_DIR="${BASH_SOURCE%/*}"
cd ${SCRIPT_DIR}

# Move to the root directory to run maven for current version.
pushd ..
GATEWAY_VERSION=$(./mvnw --quiet help:evaluate -Dexpression=project.version -DforceStdout)
popd

WORK_DIR="$(mktemp -d)"
cp ../presto-gateway-steerd/target/presto-gateway-steerd-${GATEWAY_VERSION}.tar.gz ${WORK_DIR}

# Copy the init.sql to the folder so that it can be added to docker-entrypoint-initdb.d inside Mysql container
cp ../presto-gateway-steerd/sql/steerdinit.sql ${WORK_DIR}
cp ../presto-gateway/sql/init.sql ${WORK_DIR}

cp ./hive/entrypoint.sh ${WORK_DIR}
cp ./hive/hive-site.xml ${WORK_DIR}

if [ ! "x$skipgateway" = "x1" ]; then
# copy gcloud and kubectl package repositories
   cp ../docker-gateway/google-cloud-sdk.repo ${WORK_DIR}
   cp ../docker-gateway/kubernetes.repo ${WORK_DIR}
   cp ../docker-gateway/helmtemplates/values.yaml ${WORK_DIR}
   cp ../docker-gateway/helmtemplates/presto.yaml ${WORK_DIR}

   tar -C ${WORK_DIR} -xzf ${WORK_DIR}/presto-gateway-${GATEWAY_VERSION}.tar.gz
   rm ${WORK_DIR}/presto-gateway-${GATEWAY_VERSION}.tar.gz
   cp -R bin default ${WORK_DIR}/presto-gateway-${GATEWAY_VERSION}
   docker build ${WORK_DIR} -f Dockerfile --build-arg "GATEWAY_VERSION=${GATEWAY_VERSION}" -t "steerd-gateway:${GATEWAY_VERSION}"
fi

docker build ${WORK_DIR} -f Dockerfile-postgresql -t "steerd-postgresql:${GATEWAY_VERSION}"
docker build ${WORK_DIR} -f hive/Dockerfile -t "hivemetastore:${GATEWAY_VERSION}"

rm -r ${WORK_DIR}
