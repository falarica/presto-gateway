package io.prestosql.gateway.clustermonitor;

import io.airlift.log.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import java.util.List;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/stats")
public class DefaultPrestoClusterStatsObserver
        implements ClusterStatsObserver
{
    volatile List<SteerDClusterStats> steerDClusterStats;
    Logger log = Logger.get(DefaultPrestoClusterStatsObserver.class);

    @Override
    public void observe(SteerDClusterStats steerDClusterStats)
    {
    }

    @Override
    public void observe(List<SteerDClusterStats> stats)
    {
        this.steerDClusterStats = stats;
        log.debug("observing the cluster stats: for all cluster");
        for (SteerDClusterStats stat : stats) {
            log.debug("The stats is : %s", stat);
        }
    }

    @GET
    @Path("/clusterstats")
    @Produces(APPLICATION_JSON)
    public List<SteerDClusterStats> getStats()
    {
        return steerDClusterStats;
    }
}
