package io.prestosql.gateway.routing;

import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.gateway.clustermonitor.SteerDClusterStats;
import io.prestosql.server.GatewayRequestSessionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * It will assume that the provided list is unmodified.
 * It will store the index of last routed cluster and accordingly
 * choose the next cluster.
 */
public class RoundRobinClusterRoutingRule
        extends RoutingRuleSpec
{
    public static final String NAME = "__ROUND_ROBIN_";
    // sorts on the name and keeps tab on the last index.
    AtomicInteger lastIndexOfRoutedCluster = new AtomicInteger(0);
    Logger log = Logger.get(RoundRobinClusterRoutingRule.class);

    public RoundRobinClusterRoutingRule()
    {
        super(NAME,
                RoutingRuleType.ROUNDROBIN.toString(), Collections.emptyMap());
    }

    @Override
    public List<SteerDClusterStats> apply(GatewayRequestSessionContext queryContext,
            List<SteerDClusterStats> clusterStats, Optional<Session> session)
    {
        List<SteerDClusterStats> myCopy = new ArrayList(clusterStats);
        myCopy.sort((stats1, stats2) ->
                stats1.getClusterName().hashCode() > stats2.getClusterName().hashCode() ? 1 : -1);

        while (myCopy.size() > 0) {
            int lastIndex = lastIndexOfRoutedCluster.getAndIncrement();
            if (lastIndex < myCopy.size()) {
                Collections.rotate(myCopy, myCopy.size() - lastIndex);
                log.debug("The lastindex is " + lastIndex + " the cluster is " + myCopy.get(0));
                break;
            }
            else {
                synchronized (this) {
                    if (lastIndexOfRoutedCluster.get() != 0) {
                        lastIndexOfRoutedCluster.set(0);
                    }
                }
            }
        }
        return myCopy;
    }
}
