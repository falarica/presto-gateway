package io.prestosql.gateway.routing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.Session;
import io.prestosql.gateway.clustermonitor.SteerDClusterStats;
import io.prestosql.server.GatewayRequestSessionContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CPUThresholdRoutingRule
        extends RoutingRuleSpec
{
    private static final RoutingRuleType type = RoutingRuleType.CPUUTILIZATION;

    private final int threshold;

    @JsonCreator
    public CPUThresholdRoutingRule(
            @JsonProperty("name") String name,
            @JsonProperty("properties") Map<String, String> properties)
    {
        super(name, type.toString(), properties);
        this.threshold = Integer.valueOf(properties.get("threshold"));
        //TODO: validate static rule by checking the username and clustername
        // We may need clustername atleast?
    }

    @Override
    public List<SteerDClusterStats> apply(GatewayRequestSessionContext queryContext,
            List<SteerDClusterStats> clusterStats,
            Optional<Session> session)
    {
        return null;
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
