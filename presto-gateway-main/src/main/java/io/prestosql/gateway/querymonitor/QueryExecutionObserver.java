package io.prestosql.gateway.querymonitor;

import io.prestosql.execution.QueryStats;

public interface QueryExecutionObserver
{
    public void observe(QueryStats queryStats);
}
