package io.prestosql.gateway.clustermonitor;

import java.util.List;

public interface ClusterStatsObserver
{
    void observe(SteerDClusterStats steerDClusterStats);

    void observe(List<SteerDClusterStats> stats);

    List<SteerDClusterStats> getStats();
}
