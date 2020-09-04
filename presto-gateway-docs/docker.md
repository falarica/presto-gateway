# Getting Started With Docker
## Build the UI
To build the UI for gateway, following steps need to carried out before the code is built. 

Prerequisites:  Node package manager(npm) should be pre-installed. 

- Install dependencies needed for gateway UI
```
$ npm install presto-gateway-main/src/main/ngapp/
```
- Build gateway UI
```
$ npm run-script build --prefix=presto-gateway-main/src/main/ngapp/ 
``` 
## Build the product

```bash
# build the gateway code and the code that packages the binaries
./mvnw package install -DskipTests -pl presto-gateway-main,presto-gateway 
```

## Build the image

Building of presto-server-main and presto-server is a pre-requiste before building the image. The following command builds PostgreSQL and presto gateway images. The tables of prestogateway are created using an init.sql file that is copied inside the docker-entrypoint-initdb.d folder on PostgreSQL container.

```bash
# builds the image with the latest code

docker-gateway/build-local.sh 
```

## Start the Presto gateway

Presto gateway can be launched using the following script. A PostgreSQL container, presto gateway and a hive metastore is launched. Tables and hive schema are precreated in PostgreSQL. 

```bash
# to start all services
gateway.sh up

# to start hivems, postgres
gateway.sh up 1

# to start hivems, postgres, prestoserver
gateway.sh up 2

# to start hivems, postgres, gateway
gateway.sh up 3
```
The above starts gateway with default configuration. If the configuration has to be updated, docker-gateway/default/etc folder needs to be updated. When presto-gateway is brought up, the files in this folder are mapped to the etc folder in the gateway container.

Wait for the following message log line:
```
INFO	main	io.prestosql.server.PrestoServer	======== SERVER STARTED ========
```

The Presto Gateway is now running on `localhost:8080` (the default port).

The Hive Metastore is now running on 'localhost:9083'

## Clean the Installation 

```bash 
docker-gateway/gateway.sh down
```
