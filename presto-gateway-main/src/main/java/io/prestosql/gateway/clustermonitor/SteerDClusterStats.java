package io.prestosql.gateway.clustermonitor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.server.ui.ClusterStatsResource;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SteerDClusterStats
{
    private ClusterStatsResource.ClusterStats prestoClusterStats;
    private boolean healthy;
    private String clusterName;
    private String clusterUrl;
    private String location;

    public SteerDClusterStats()
    {
    }

    @JsonCreator
    public SteerDClusterStats(
            @JsonProperty("prestoClusterStats") ClusterStatsResource.ClusterStats prestoClusterStats,
            @JsonProperty("healthy") boolean healthy,
            @JsonProperty("clusterName") String clusterName,
            @JsonProperty("clusterUrl") String clusterUrl,
            @JsonProperty("location") String location)
    {
        this.prestoClusterStats = prestoClusterStats;
        this.healthy = healthy;
        this.clusterName = clusterName;
        this.clusterUrl = clusterUrl;
        this.location = location;
    }

    @JsonProperty
    public ClusterStatsResource.ClusterStats getPrestoClusterStats()
    {
        return this.prestoClusterStats;
    }

    @JsonProperty
    public String getClusterName()
    {
        return clusterName;
    }

    @JsonProperty
    public String getClusterUrl()
    {
        return clusterUrl;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    public void setLocation(String location)
    {
        this.location = location;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SteerDClusterStats that = (SteerDClusterStats) o;
        return
                (prestoClusterStats != null ? prestoClusterStats.equals(that.prestoClusterStats) : true) &&
                        healthy == that.healthy &&
                        Objects.equals(clusterName, that.clusterName) &&
                        Objects.equals(clusterUrl, that.clusterUrl) &&
                        Objects.equals(location, that.location);
    }

    @Override
    public String toString()
    {
        com.google.common.base.MoreObjects.ToStringHelper helper = toStringHelper(this)
                .add("healthy", healthy)
                .add("clusterId", clusterName)
                .add("clusterUrl", clusterUrl)
                .add("location", location).omitNullValues();
        if (prestoClusterStats != null) {
            return helper.add("activeCoordinators", prestoClusterStats.getActiveCoordinators())
                    .add("activeWorkers", prestoClusterStats.getActiveWorkers())
                    .add("blockedQueries", prestoClusterStats.getBlockedQueries())
                    .add("queuedQueries", prestoClusterStats.getQueuedQueries())
                    .add("reservedMemory", prestoClusterStats.getReservedMemory())
                    .add("runningDrivers", prestoClusterStats.getRunningDrivers())
                    .add("runningQueries", prestoClusterStats.getRunningQueries())
                    .add("totalAvailableProcessors", prestoClusterStats.getTotalAvailableProcessors())
                    .add("totalCpuTimeSecs", prestoClusterStats.getTotalCpuTimeSecs())
                    .add("totalInputBytes", prestoClusterStats.getTotalInputBytes())
                    .add("totalInputRows", prestoClusterStats.getTotalInputRows()).toString();
        }
        return helper.toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(prestoClusterStats, healthy, clusterName, clusterUrl, location);
    }

    public void setClusterName(String name)
    {
        this.clusterName = name;
    }

    public void setHealthy(boolean b)
    {
        this.healthy = b;
    }

    public void setClusterUrl(String clusterUrl)
    {
        this.clusterUrl = clusterUrl;
    }

    public void setPrestoClusterStats(ClusterStatsResource.ClusterStats result)
    {
        this.prestoClusterStats = result;
    }
}
