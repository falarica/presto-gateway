package io.prestosql.gateway.routing;

public enum RoutingRuleType
{
    COLOCATION,
    RANDOMCLUSTER,
    ROUNDROBIN,
    CPUUTILIZATION,
    RUNNINGQUERY,
    QUEUEDQUERY,
    USERPREFERENCE,
    CATALOGPREFERENCE,
    CLOUDBURST
}
