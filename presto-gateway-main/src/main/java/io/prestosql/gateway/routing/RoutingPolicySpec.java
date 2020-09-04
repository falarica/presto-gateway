package io.prestosql.gateway.routing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class RoutingPolicySpec
{
    public static final String DEFAULT_POLICY_NAME = "_DEFAULT_ROUTING_POLICY_";
    private final String name;

    private final List<String> routingRulesSpec;

    @JsonCreator
    public RoutingPolicySpec(@JsonProperty("name") String name,
            @JsonProperty("routingRules") List<String> routingRules)
    {
        this.name = requireNonNull(name, "name is null");
        this.routingRulesSpec = ImmutableList.copyOf(requireNonNull(routingRules, "routingRules is null"));
    }

    @JsonProperty
    public List<String> getRoutingRules()
    {
        return routingRulesSpec;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }
}
