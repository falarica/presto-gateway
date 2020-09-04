To Run:
docker run -d -p 3306:3306  --name mysqldb -e MYSQL_ROOT_PASSWORD=root123 -e MYSQL_DATABASE=prestogateway -d mysql:5.7
docker start mysqldb

    mysql -uroot -proot123 -h127.0.0.1 -Dprestogateway

CREATE TABLE IF NOT EXISTS clusters ( name VARCHAR(256) PRIMARY KEY,
   location VARCHAR (256),
   coordinator_url VARCHAR (256),
   active BOOLEAN );

CREATE TABLE IF NOT EXISTS executing_queries (query_id varchar(256),
 query_text text, cluster_url varchar(256),
  user_name varchar (256), source varchar(256),
   created varchar(256));


CREATE TABLE IF NOT EXISTS catalogs (
    name VARCHAR(256), location VARCHAR (256));
 
CREATE TABLE IF NOT EXISTS clusters_catalogs(cluster_name varchar(256),
 catalog_name varchar(256));

CREATE TABLE IF NOT EXISTS query_stats(query_id varchar(256) primary key,
 cluster_url varchar(256), query_info text);

CREATE TABLE IF NOT EXISTS query_stats_json(query_id varchar(256) primary key,
 cluster_url varchar(256), query_info jsonb);


CREATE TABLE IF NOT EXISTS routingpolicy(name VARCHAR(256) PRIMARY KEY, ruleids jsonb);

CREATE TABLE IF NOT EXISTS routingrules(name VARCHAR(256) PRIMARY KEY, type VARCHAR(256), properties jsonb);


create table groups(name varchar(255) PRIMARY KEY);

create table users(user_name varchar(255) PRIMARY KEY, password varchar(255));

create table users_groups(id serial, user_name varchar(255) , group_name varchar(255));




Add a Cluster:

curl -X POST http://localhost:8088/add?entityType=CATALOG -d '{  "name": "cluster1hive", "location": "mumbai" }'
curl -X GET http://localhost:8088/catalogs
curl -X POST http://localhost:8088/add?entityType=CLUSTER -d 
'{ "name": "cluster1", "clusterUrl": "http://localhost:8080", "active": true, "location": "mumbai",
 "catalogs":{"hive":"mumbai","tpch":"mumbai","tpcds":"mumbai"} }'

curl -X POST http://localhost:8088/add?entityType=CLUSTER -d '{  "name": "presto2", "clusterUrl": "http://presto1.lyft.com", "active": true, "location": "mumbai" }'
curl -X GET http://localhost:8088/clusters
curl -X POST http://localhost:8088/add?entityType=CATALOG -d '{  "name": "presto2hive", "location": "mumbai" }'
curl -X GET http://localhost:8088/catalogs
curl -X DELETE http://localhost:8088/catalog/presto2hive
curl -X DELETE http://localhost:8088/catalog/presto2
curl -X DELETE http://localhost:8088/delete/catalog/tpch
curl -X DELETE http://localhost:8088/delete/cluster/presto2

curl -X POST http://localhost:8088/update?entityType=CLUSTER -d {"name":"presto3","location":"mumbai","clusterUrl":"http://localhost:9090","active":true}


{ "name" : "rule1", "type":"ColocationRule", "attributes" : { } }

{ "name" : "rule2", "type":"RoundRobinRoutingRule", "attributes" : { } }

{ "name" : "rule3", "type":"CPUUtilizationRule",  "attributes" : { "threshold" :"80" } }

{ "name" : "rule4", "type":"RunningQueryBasedRule", "attributes" : { "numQuery" :"10" } }

{ "name" : "rule5", "type":"QueuedQueryBasedRule", "attributes" : { "numQuery" :"10" } }

{ "name" : "rule6", "type":"UserPreferenceRule", "attributes" : { "username1": ["clustername1"], "username2": ["clustername1", "clustername2"] }}

{ "name" : "rule7", "type":"CatalogPreferenceule", "attributes" : { "catalog1": ["clustername1"], "catalog2": ["clustername1", "clustername2"] }}

{ "name" : "rule8", "type":"CloudBurstrule", "attributes" : { "OverallUtilizationThreshold": "80", "OverallQueueSizeThreshold": "50"}}



curl -X POST http://localhost:8089/add?entityType=GROUP -d '{  "name": "dev"}'
curl -X POST http://localhost:8089/add?entityType=GROUP -d '{  "name": "test"}'

curl -X POST http://localhost:8089/add?entityType=USER -d '{  "name": "alice", "password": "alice", "group":["dev"] }'

curl -X POST http://localhost:8089/add?entityType=USER -d '{  "name": "alice", "password": "alice", "group":["test"] }'

curl -X POST http://localhost:8089/update?entityType=USER -d '{ "name": "alice", "group":["test","dev"] }'

curl -X POST http://localhost:8089/add?entityType=USER -d '{  "name": "alice", "password": "alice", "group":["test"] }'

curl -X POST http://localhost:8089/add?entityType=USER -d '{  "name": "alice", "password": "alice", "group":["test"] }'


curl -X POST http://localhost:8089/add?entityType=ROUTINGPOLICY -d '{ "name" : "polic7", "routingRules" : [ "ColocationRule", "LeastLoaded", "Random" ]}'

{ "name" : "rule1", "type":"ColocationRule", "attributes" : { } }

{ "name" : "rule2", "type":"RoundRobinRoutingRule", "attributes" : { } }

{ "name" : "rule3", "type":"CPUUtilizationRule",  "attributes" : { "threshold" :"80" } }

{ "name" : "rule4", "type":"RunningQueryBasedRule", "attributes" : { "numQuery" :"10" } }

{ "name" : "rule5", "type":"QueuedQueryBasedRule", "attributes" : { "numQuery" :"10" } }

{ "name" : "rule6", "type":"UserPreferenceRule", "attributes" : { "username1": ["clustername1"], "username2": ["clustername1", "clustername2"] }}

{ "name" : "rule7", "type":"CatalogPreferenceule", "attributes" : { "catalog1": ["clustername1"], "catalog2": ["clustername1", "clustername2"] }}

{ "name" : "rule8", "type":"CloudBurstrule", "attributes" : { "OverallUtilizationThreshold": "80", "OverallQueueSizeThreshold": "50"}}



curl -X POST http://localhost:8089/add?entityType=RESOURCEGROUP -d '{
	        "id" : 1	
            "name": "global",
            "softMemoryLimit": "100%",
            "hardConcurrencyLimit":1,
            "maxQueued": 100,
            "schedulingPolicy": "fair"
        }'

curl -X POST http://localhost:8089/add?entityType=RGSELECTOR -d '{
	        "rsid" : 1,
            "source": ".*pipeline.*"
        }

curl -X POST http://localhost:8089/add?entityType=RGSELECTOR -d '{
	        "rsid" : 2,
            "user": "bob"
        }'

curl -X POST http://localhost:8089/add?entityType=QUOTAPERIOD -d '{
	        "cpuQuotaPeriod": "5m"
    }'

curl -X POST http://localhost:8089/add?entityType=USERPREFERENCE -d  '{
        "name" : "rule6",
        "properties" : { "username1": "clustername1", "username2": "clustername1" }
    }'

curl -X POST http://localhost:8089/add?entityType=USERPREFERENCE -d  '{
        "name" : "rule6",
        "properties" : { "username1": ["clustername1"],
        "username2": ["clustername1", "clustername2"] }
    }'

curl -X POST http://localhost:8089/add?entityType=USERPREFERENCE -d  '{
        "name" : "rule66",
        "properties" : { "username1": "clustername1", "username2": "clustername1" }
    }'


curl -X POST http://localhost:8089/add?entityType=ROUTINGPOLICY -d '{
            "name" : "leastloaded", 
            "routingRules" : [ "ColocationRule", "LeastLoaded", "Random" ]
        }'


curl -X POST http://localhost:8089/add?entityType=RPSELECTOR -d '{
	        "rpname" : "leastloaded",
            "group": "test"
        }'

curl -X POST http://localhost:8089/add?entityType=ROUTINGPOLICY -d '{ 
            "name" : "colocated",
             "routingRules" : [ "ColocationRule", "Random" ]
        }'

curl -X POST http://localhost:8089/add?entityType=RPSELECTOR -d '{
	        "rpname" : "colocated",
            "source": "*pipeline*"
       }'
