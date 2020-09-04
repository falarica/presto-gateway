package io.prestosql.gateway.routing;

import io.prestosql.Session;
import io.prestosql.gateway.clustermonitor.SteerDClusterStats;
import io.prestosql.server.GatewayRequestSessionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * This will break the original sorting order provided.
 */
public final class RandomClusterRoutingRule
        extends RoutingRuleSpec
{
    public static final String NAME = "__RANDOM__";

    public RandomClusterRoutingRule()
    {
        super(NAME, RoutingRuleType.RANDOMCLUSTER.toString(),
                Collections.emptyMap());
    }

    @Override
    public List<SteerDClusterStats> apply(GatewayRequestSessionContext queryContext,
            List<SteerDClusterStats> clusterStats, Optional<Session> session)
    {
        // make a copy
        List<SteerDClusterStats> modifiedClusters = new ArrayList(clusterStats);
        Collections.shuffle(modifiedClusters);
        return modifiedClusters;
        //List urls = clusterStats.stream().map(cd -> cd.getClusterUrl()).collect(Collectors.toList());
        //Collections.shuffle(urls);
        //return urls;
    }
}
