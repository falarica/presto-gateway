package io.prestosql.gateway;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/")
public class ClusterResource
{
    private final MultiClusterManager multiClusterManager;
    private final QueryHistoryManager queryHistoryManager;

    @Inject
    public ClusterResource(MultiClusterManager cmgr, QueryHistoryManager qmgr)
    {
        this.multiClusterManager = requireNonNull(cmgr, "clusterMgr is null");
        this.queryHistoryManager = requireNonNull(qmgr, "queryHistoryMgr is null");
    }

    @GET
    @Path("/clusters")
    @Produces(APPLICATION_JSON)
    public Response getClusters()
    {
        return Response.ok().entity(this.multiClusterManager.getAllClusters()).build();
    }

    @GET
    @Path("/routingpolicy")
    @Produces(APPLICATION_JSON)
    public Response getRoutingPolicies()
    {
        return Response.ok().entity(this.multiClusterManager.getAllRoutingPolicies()).build();
    }

    @GET
    @Path("/routingrules")
    @Produces(APPLICATION_JSON)
    public Response getRoutingRules()
    {
        return Response.ok().entity(this.multiClusterManager.getAllRoutingRules()).build();
    }

    @GET
    @Path("/executingquery")
    @Produces(APPLICATION_JSON)
    public Response getQueries()
    {
        return Response.ok().entity(this.queryHistoryManager.fetchQueryHistory()).build();
    }
}
