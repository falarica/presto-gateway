package io.prestosql.gateway.querymonitor;

public interface QueryExecutionMonitor
{
    void monitorQueryExecution(QueryExecutionContext context);

    void start();

    void stop();

    void scheduleQueryHistoryCleanup();
}
