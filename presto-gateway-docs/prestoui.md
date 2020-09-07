# Presto Gateway UI
Presto Gateway UI provides an easy web interface for managing multiple presto clusters for the Gateway. For users, it provides single interface to see their query across different PrestoSQL/DB clusters.

# Login
Presto Gateway supports user authentication. If authentication is not enabled, just provide username to login to the webui.
![Alt text](images/login.png?raw=true "Login")

# DashBoard
The DashBoard show the state of all the clusters combined. e.g RunningQuery shows the total running queries across all the clusters added to the Presto Gateway.
![Alt text](images/dashboard.png?raw=true "Dashboard")

### Query Stats

The QueryStats show the brief stats of the Query, e.g user, queryid, query text etc.
![Alt text](images/query-stat.png?raw=true "Query Stats")

# Clusters
It shows the list of Presto clusters added to the PrestoGateway. You can add/delete a presto cluster as well.
![Alt text](images/clusters.png?raw=true "Clusters")

##### Adding a Cluster
To add a cluster, you need to provide a unique name, co-ordinator url, a location and its admin user name and password.

In case, the Presto cluster is not secured, you can avoid providing the admin user name and password.
![Alt text](images/add-cluster.png?raw=true "Adding a Cluster")

# RoutingPolicy/RoutingRule

RoutingPolicy determines the criteria for a cluster to be qualified for a query.
The default RoutingPolicy is RANDOM, which means any cluster can be chosen randomly for the query to be executed.
![Alt text](images/routingpolicy.png?raw=true "RoutingPolicy/RoutingRule")

Some other RoutingPolicy are:
###### ROUNDROBIN
The cluster next in queue will be chosen for the incoming query.
###### QUEUEDQUERY
It takes a property numQueuedQuery which filters the clusters which has more than numQueuedQuery at that instant.
###### RUNNINGQUERY
It takes a property numRunningQuery which filters the clusters which has more than numRunningQuery at that instant.

#### Adding a RoutingRule
![Alt text](images/add-rule.png?raw=true "Adding a RoutingRule")

#### Deleting a RoutingRule
![Alt text](images/delete-rule.png?raw=true "Deleting a RoutingRule")

#### Editing the RoutingPolicy
![Alt text](images/edit-policy.png?raw=true "Editing the RoutingPolicy")
