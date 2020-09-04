package io.prestosql.gateway.routing;

import java.util.List;

public interface RoutingPPolicy
{
    public List<RoutingRule> getRules();
}
