package io.prestosql.gateway.routing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.Session;
import io.prestosql.gateway.clustermonitor.SteerDClusterStats;
import io.prestosql.server.GatewayRequestSessionContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class RoutingRuleSpec
        implements RoutingRule
{
    private final String name;
    private final String type;
    private final Map<String, String> properties;

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }

    @JsonCreator
    public RoutingRuleSpec(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("properties") Map<String, String> properties)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.properties = requireNonNull(properties, "properties is null");
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof RoutingRuleSpec)) {
            return false;
        }
        RoutingRuleSpec that = (RoutingRuleSpec) other;

        return (getName().equals(that.getName()) && this.getType().equals(that.getType()) &&
                getProperties().equals(that.getProperties()));
    }

    public void validateProperties()
    {
    }

    @Override
    public int hashCode()
    {
        return this.name.hashCode() * this.type.hashCode();
    }

    @Override
    public List<SteerDClusterStats> apply(GatewayRequestSessionContext queryContext,
            List<SteerDClusterStats> clusterStats,
            Optional<Session> session)
    {
        return null;
    }
}
