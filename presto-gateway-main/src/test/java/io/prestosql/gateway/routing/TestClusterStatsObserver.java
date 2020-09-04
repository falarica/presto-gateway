package io.prestosql.gateway.routing;

import io.prestosql.gateway.clustermonitor.ClusterStatsObserver;
import io.prestosql.gateway.clustermonitor.SteerDClusterStats;

import java.util.List;

public class TestClusterStatsObserver
        implements ClusterStatsObserver
{
    private List<SteerDClusterStats> steerDClusterStats;

    public TestClusterStatsObserver(List<SteerDClusterStats> steerDClusterStats)
    {
        this.steerDClusterStats = steerDClusterStats;
    }

    @Override
    public void observe(SteerDClusterStats steerDClusterStats)
    {
        throw new UnsupportedOperationException("not supported in TestClusterStatsObserver");
    }

    @Override
    public void observe(List<SteerDClusterStats> stats)
    {
        throw new UnsupportedOperationException("not supported in TestClusterStatsObserver");
    }

    @Override
    public List<SteerDClusterStats> getStats()
    {
        return this.steerDClusterStats;
    }
}
