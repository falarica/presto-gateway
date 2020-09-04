package io.prestosql.gateway.routing;

import io.prestosql.Session;
import io.prestosql.gateway.clustermonitor.SteerDClusterStats;
import io.prestosql.server.GatewayRequestSessionContext;

import java.util.List;
import java.util.Optional;

/**
 * RoutingRule will take a QueryContext and ClusterContext and a list of Clusters
 * It will work on a list of cluster and will reorder it according to the rules.
 * Some rules can remove the cluster from the list.
 */
@FunctionalInterface
public interface RoutingRule
{
    List<SteerDClusterStats> apply(GatewayRequestSessionContext queryContext,
            List<SteerDClusterStats> clusterStats,
            Optional<Session> session);
}
