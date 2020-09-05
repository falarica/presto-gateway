# Presto Gateway UI
Presto Gateway UI provides an easy web interface for managing multiple presto clusters for the Gateway. For users, it provides single
interface to see their query across different PrestoSQL/DB clusters.
#Login
Presto Gateway supports user authentication. If authentication is not enabled, just provide username to login to the webui.
abc
#DashBoard
The DashBoard show the state of all the clusters combined. e.g RunningQuery shows the total running queries
across all the clusters added to the Presto Gateway.

### Query Stats

The QueryStats show the brief stats of the Query, e.g user, queryid, query text etc.

#Clusters
It shows the list of Presto clusters added to the PrestoGateway. You can add/delete a presto cluster as well.

##### Adding a Cluster
To add a cluster, you need to provide a unique name, co-ordinator url,
 a location and its admin user name and password.

In case, the Presto cluster is not secured, you can avoid providing the admin user name and password.

#RoutingPolicy/RoutingRule

RoutingPolicy determines the criteria for a cluster to be qualified for a query.
The default RoutingPolicy is RANDOM, which means any cluster can be chosen randomly for the query to be executed.

Some other RoutingPolicy are:
###### ROUNDROBIN
The cluster next in queue will be chosen for the incoming query.
###### QUEUEDQUERY
It takes a property numQueuedQuery which filters the clusters which has more than numQueuedQuery at that instant.
###### RUNNINGQUERY
It takes a property numRunningQuery which filters the clusters which has more than numRunningQuery at that instant.

#### Adding a RoutingRule

#### Deleting a RoutingRule

#### Editing the RoutingPolicy
