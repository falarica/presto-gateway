package io.prestosql.gateway.routing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.Session;
import io.prestosql.gateway.clustermonitor.SteerDClusterStats;
import io.prestosql.server.GatewayRequestSessionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class QueuedQueryRoutingRule
        extends RoutingRuleSpec
{
    private static final RoutingRuleType type = RoutingRuleType.QUEUEDQUERY;
    public static final String QUEUED_QUERY_PROP_NAME = "numQueuedQueries";
    private final int threshold;

    @JsonCreator
    public QueuedQueryRoutingRule(
            @JsonProperty("name") String name,
            @JsonProperty("properties") Map<String, String> properties)
    {
        super(name, type.toString(), properties);
        this.threshold = Integer.valueOf(properties.get(QUEUED_QUERY_PROP_NAME));
    }

    @Override
    public List<SteerDClusterStats> apply(GatewayRequestSessionContext queryContext,
            List<SteerDClusterStats> clusterStats,
            Optional<Session> session)
    {
        List<SteerDClusterStats> myCopy = new ArrayList(clusterStats);
        return myCopy.stream()
                .filter(steerDClusterStats -> {
                    if (steerDClusterStats.getPrestoClusterStats() != null) {
                        return steerDClusterStats.getPrestoClusterStats().getQueuedQueries() <= getThreshold();
                    }
                    return true;
                }).collect(Collectors.toList());
    }

    public int getThreshold()
    {
        return this.threshold;
    }

    @Override
    public void validateProperties()
    {
    }
}
