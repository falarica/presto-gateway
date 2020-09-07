# Presto Gateway Open Source APIs Postman link available here :

https://documenter.getpostman.com/view/12245775/TVCY4WXH

&nbsp;

# Presto Gateway Open Source APIs available here :

<b>1) GET Cluster stats :</b>

    $ curl --location --request GET 'http://localhost:8089/stats/clusterstats'

<b>2) Add Cluster :</b>

    $ curl --location --request POST 'http://localhost:8089/add?entityType=CLUSTER' --data-raw '{ "name": "cluster1", "clusterUrl": "https://localhost:8081", "active": true, "adminName": "admin", "adminPassword": "admin", "location": "mumbai"}'

<b>3) Update Cluster :</b>

    $ curl --location --request POST 'http://localhost:8089/update?entityType=CLUSTER' \ --data-raw '{ "name": "cluster1", "clusterUrl": "https://localhost:8081", "active": true, "adminName": "admin", "adminPassword": "admin", "location": "mumbai"}'

<b>4) GET Clusters List :</b>

    $ curl --location --request GET 'http://localhost:8089/clusters'

<b>5) DEL Delete Cluster: :</b>
    
    $ curl --location --request DELETE 'http://localhost:8089/delete/cluster/cluster1'

<b>6) GET Queries List :</b>

    $ curl --location --request GET 'http://localhost:8089/querydetails2'

<b>7) POST Update Policy :</b>

    $ curl --location --request POST 'http://localhost:8089/update?entityType=ROUTINGPOLICY' --data-raw '{"name":"leastloaded1","routingRules":["rule1"]}'

<b>8) GET List Routing Policy :</b>

    $ curl --location --request GET 'http://localhost:8089/routingpolicy'

<b>9) DEL Delte Routing Policy:</b>

    $ curl --location --request DELETE 'http://localhost:8089/delete/routingpolicy/{name}'

<b>10) GET List Routing Rules :</b>

    $ curl --location --request GET 'http://localhost:8089/routingrules'

<b>11) POST Add ROUNDROBIN Rule :</b>

    $ curl --location --request POST 'http://localhost:8089/add?entityType=ROUNDROBIN' \
    --data-raw '{
    "name": "rule123",
    "type": "ROUNDROBIN"
}'

<b>12) POST Add RANDOMCLUSTER Rule :</b>

    $ curl --location --request POST 'http://localhost:8089/add?entityType=RANDOMCLUSTER' \
    --data-raw '{
    "name": "rule1",
    "type": "RANDOMCLUSTER"
}'

<b>13) POST Add QUEUEDQUERY rule :</b>

    $ curl --location --request POST 'http://localhost:8089/add?entityType=QUEUEDQUERY' \
    --data-raw '{
    "name": "rule1",
    "type": "QUEUEDQUERY",
    "properties": {
        "numQueuedQueries": "10"
    }
}'

<b>14) POST Add RUNNINGQUERY rule :</b>

    $ curl --location --request POST 'http://localhost:8089/add?entityType=RUNNINGQUERY' \
    --data-raw '{
    "name": "rule1",
    "type": "QUEUEDQUERY",
    "properties": {
        "numRunningQueries": "10"
    }
}'

<b>15) DEL Delete Routing Rule :</b>

    $ curl --location --request DELETE 'http://localhost:8089/delete/routingrule/{name}'


