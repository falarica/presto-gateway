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

public class RunningQueryRoutingRule
        extends RoutingRuleSpec
{
    private static final RoutingRuleType TYPE = RoutingRuleType.RUNNINGQUERY;
    public static final String NUMQUERIES_PROP_NAME = "numRunningQueries";
    private final int threshold;

    @JsonCreator
    public RunningQueryRoutingRule(
            @JsonProperty("name") String name,
            @JsonProperty("properties") Map<String, String> properties)
    {
        super(name, TYPE.toString(), properties);
        this.threshold = Integer.parseInt(properties.get(NUMQUERIES_PROP_NAME));
    }

    // currently provides sorted list.
    // we have an option to throw exception if threshold based rule.
    @Override
    public List<SteerDClusterStats> apply(GatewayRequestSessionContext queryContext,
            List<SteerDClusterStats> clusterStats,
            Optional<Session> session)
    {
        List<SteerDClusterStats> myCopy = new ArrayList(clusterStats);

        return myCopy.stream()
                .filter(steerDClusterStats -> {
                    if (steerDClusterStats.getPrestoClusterStats() != null) {
                        return steerDClusterStats.getPrestoClusterStats().getRunningQueries() <= getThreshold();
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
