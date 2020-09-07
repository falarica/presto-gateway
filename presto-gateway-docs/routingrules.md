# Routing Rules 

## Purpose
Routing Rules decide the cluster which can qualify for a query. In presto-gateway there
are 4 different RoutingRules provided.
You can choose any one of the rules to be the Routing Policy.

## Types
- **Random**    
        Choose an available cluster randomly out of the list of clusters added.
- **RoundRobin**    
        Choose an available cluster in round robin fashion for each of the query
         handed over to the RoutingManager.
- **QueuedQuery**    
        Choose an available cluster where less than specified number of queries are queued.
- **RunningQuery**    
        Choose an available cluster where less than specified number of queries are running.
