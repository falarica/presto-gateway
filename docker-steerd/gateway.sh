#!/usr/bin/env bash

set -eo pipefail

# Retrieve the script directory.
SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
pushd ${SCRIPTPATH}

# Move to the root directory to run maven for current version.
pushd ..
export GATEWAY_VERSION=$(./mvnw --quiet help:evaluate -Dexpression=project.version -DforceStdout)
popd
export HADOOP_HIVE_ENV_FILE=${SCRIPTPATH}/hive/hadoop-hive.env
export PGPASSWORD=root123
export GATEWAY_CONFIG_DIR=${SCRIPTPATH}/default/etc
export PRESTO_CONFIG_DIR=${SCRIPTPATH}/default/presto/etc

# create a temporary directory that can be mounted on prestoserver
mkdir -p /tmp/filedata
chmod 777 /tmp/filedata

usage='
gateway.sh up
    starts all services
gateway.sh up 1
    starts hivems, postgres
gateway.sh up 2
    starts hivems, postgres, prestoserver
gateway.sh up 3
    starts hivems, postgres, gateway
gateway.sh down
    stops everything
'
if [ -z $1 ]; then
    echo "pass either 'up' or 'down' as the parameter"
    echo "$usage"
    exit 0
elif [ "$1" = "up" ]; then
    if [ -z $2 ]; then
       docker-compose  up -d  hivemetastore steerdmetastore prestoserver steerdgateway
    elif [ "$2" = "1" ]; then
        docker-compose up -d hivemetastore steerdmetastore
    elif [ "$2" = "2" ]; then
        docker-compose up -d hivemetastore steerdmetastore prestoserver
    elif [ "$2" = "3" ]; then
        docker-compose up -d hivemetastore steerdmetastore steerdgateway
    else
       echo "valid values for second parameter 1, 2, 3"
       echo "$usage"
       exit 0
    fi
elif [ "$1" = "down" ]; then
        docker-compose down -v
else
    echo "pass either 'up' or 'down' as the parameter"
    echo "$usage"
    exit 0
fi

popd
