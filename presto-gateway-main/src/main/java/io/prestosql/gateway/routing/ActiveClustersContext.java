package io.prestosql.gateway.routing;

import io.prestosql.gateway.clustermonitor.SteerDClusterStats;

import java.util.List;

/**
 * Maintains the active clusters details and stats.
 * Cluster details are present in the database and stats
 * are maintained by the ActiveClusterMonitor.
 */

public class ActiveClustersContext
{
    List<SteerDClusterStats> steerDClusterStats;

    public ActiveClustersContext(List<SteerDClusterStats> steerDClusterStats)
    {
        this.steerDClusterStats = steerDClusterStats;
    }

    public List<SteerDClusterStats> getClusterDetails()
    {
        return steerDClusterStats;
    }

    public List<SteerDClusterStats> getClusterStats()
    {
        return steerDClusterStats;
    }
}
