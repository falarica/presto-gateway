package io.prestosql.gateway;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Strings;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.prestosql.gateway.clustermonitor.ClusterMonitor;
import io.prestosql.gateway.clustermonitor.PullBasedPrestoClusterMonitor;
import io.prestosql.gateway.persistence.ClusterDetail;
import io.prestosql.gateway.routing.CPUThresholdRoutingRule;
import io.prestosql.gateway.routing.QueuedQueryRoutingRule;
import io.prestosql.gateway.routing.RandomClusterRoutingRule;
import io.prestosql.gateway.routing.RoundRobinClusterRoutingRule;
import io.prestosql.gateway.routing.RoutingManager;
import io.prestosql.gateway.routing.RoutingPolicySelectorSpec;
import io.prestosql.gateway.routing.RoutingPolicySpec;
import io.prestosql.gateway.routing.RoutingRuleSpec;
import io.prestosql.gateway.routing.RunningQueryRoutingRule;
import io.prestosql.gateway.routing.UserPreferenceRoutingRule;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import java.io.IOException;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/")
public class ClusterEditorResource
{
    protected final ClusterMonitor statspuller;

    protected enum EntityType
    {
        CLUSTER,
        THRESHOLDROUTINGRULE,
        ROUTINGPOLICY_SELECTORSPEC,
        ROUTINGPOLICY,
        COLOCATION,
        ROUNDROBIN,
        CPUUTILIZATION,
        RUNNINGQUERY,
        QUEUEDQUERY,
        RANDOMCLUSTER,
        USERPREFERENCE
    }

    protected final MultiClusterManager multiClusterManager;
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected static final JsonCodec<RoutingPolicySelectorSpec> RP_SELECTOR_CODEC = new JsonCodecFactory(
            () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .jsonCodec(RoutingPolicySelectorSpec.class);

    private Logger log = Logger.get(ClusterEditorResource.class);

    protected final RoutingManager routingManager;
    // we need to separate the rule creation here for enterprise version

    @Inject
    public ClusterEditorResource(MultiClusterManager mgr, RoutingManager routingMgr, ClusterMonitor monitor)
    {
        this.multiClusterManager = requireNonNull(mgr, "clusterMgr is null");
        OBJECT_MAPPER.registerModule(new Jdk8Module());
        this.routingManager = requireNonNull(routingMgr, "clusterMgr is null");
        this.statspuller = monitor;
    }

    @Path("/add")
    //@Consumes(APPLICATION_JSON)
    @POST
    public Response addEntity(@QueryParam("entityType") String entityTypeStr, String entityInfo)
    {
        if (Strings.isNullOrEmpty(entityTypeStr)) {
            throw new WebApplicationException("EntityType can not be null");
        }
        EntityType entityType = EntityType.valueOf(entityTypeStr);
        try {
            switch (entityType) {
                case CLUSTER: {
                    ClusterDetail clusterDetail = OBJECT_MAPPER.readValue(entityInfo, ClusterDetail.class);
                    multiClusterManager.addCluster(clusterDetail);
                    ((PullBasedPrestoClusterMonitor) statspuller).pullClusterStats();
                    break;
                }
                case ROUTINGPOLICY:
                    RoutingPolicySpec policySpec = OBJECT_MAPPER.readValue(entityInfo, RoutingPolicySpec.class);
                    routingManager.addRoutingPolicy(policySpec);
                    break;
                case ROUTINGPOLICY_SELECTORSPEC: {
                    RoutingPolicySelectorSpec selectorSpec = null;
                    try {
                        selectorSpec = RP_SELECTOR_CODEC.fromJson(entityInfo);
                    }
                    catch (IllegalArgumentException e) {
                        throwJSONMappingException(e);
                    }
                    multiClusterManager.addRoutingPolicySelector(selectorSpec);
                    break;
                }
                case USERPREFERENCE: {
                    RoutingRuleSpec tSpec = OBJECT_MAPPER.readValue(entityInfo, UserPreferenceRoutingRule.class);
                    routingManager.addRoutingRule(tSpec);
                    break;
                }
                case CPUUTILIZATION: {
                    CPUThresholdRoutingRule tSpec = OBJECT_MAPPER.readValue(entityInfo, CPUThresholdRoutingRule.class);
                    routingManager.addRoutingRule(tSpec);
                    break;
                }
                case QUEUEDQUERY: {
                    QueuedQueryRoutingRule tSpec = OBJECT_MAPPER.readValue(entityInfo, QueuedQueryRoutingRule.class);
                    routingManager.addRoutingRule(tSpec);
                    break;
                }
                case RUNNINGQUERY: {
                    RunningQueryRoutingRule tSpec = OBJECT_MAPPER.readValue(entityInfo, RunningQueryRoutingRule.class);
                    routingManager.addRoutingRule(tSpec);
                    break;
                }
                case RANDOMCLUSTER: {
                    routingManager.addRoutingRule(new RandomClusterRoutingRule());
                    break;
                }
                case ROUNDROBIN: {
                    routingManager.addRoutingRule(new RoundRobinClusterRoutingRule());
                    break;
                }
            }
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new WebApplicationException(e);
        }
        return Response.ok().build();
    }

    protected void throwJSONMappingException(IllegalArgumentException e)
    {
        Throwable cause = e.getCause();
        if (cause instanceof UnrecognizedPropertyException) {
            UnrecognizedPropertyException ex = (UnrecognizedPropertyException) cause;
            String message = format("Unknown property at line %s:%s: %s",
                    ex.getLocation().getLineNr(),
                    ex.getLocation().getColumnNr(),
                    ex.getPropertyName());
            throw new IllegalArgumentException(message, e);
        }
        if (cause instanceof JsonMappingException) {
            // remove the extra "through reference chain" message
            if (cause.getCause() != null) {
                cause = cause.getCause();
            }
            throw new IllegalArgumentException(cause.getMessage(), e);
        }
        throw e;
    }

    @Path("/update")
    @Consumes(APPLICATION_JSON)
    @POST
    public Response updateEntity(@QueryParam("entityType") String entityTypeStr, String entityInfo)
    {
        if (Strings.isNullOrEmpty(entityTypeStr)) {
            throw new WebApplicationException("EntityType can not be null");
        }
        EntityType entityType = EntityType.valueOf(entityTypeStr);

        try {
            switch (entityType) {
                case CLUSTER:
                    ClusterDetail clusterDetail = OBJECT_MAPPER.readValue(entityInfo, ClusterDetail.class);
                    multiClusterManager.updateCluster(clusterDetail);
                    ((PullBasedPrestoClusterMonitor) statspuller).pullClusterStats();
                    break;
                case ROUTINGPOLICY: {
                    RoutingPolicySpec policy = OBJECT_MAPPER.readValue(entityInfo, RoutingPolicySpec.class);
                    routingManager.updatePolicy(policy);
                    multiClusterManager.updatePolicy(policy);
                    break;
                }
                case QUEUEDQUERY: {
                    QueuedQueryRoutingRule queuedQueryRoutingRule = OBJECT_MAPPER.readValue(entityInfo, QueuedQueryRoutingRule.class);
                    multiClusterManager.updateQueuedQueryBasedRule(queuedQueryRoutingRule);
                    break;
                }
                case RUNNINGQUERY: {
                    RunningQueryRoutingRule runningQueryRoutingRuleSpec = OBJECT_MAPPER.readValue(entityInfo, RunningQueryRoutingRule.class);
                    multiClusterManager.updateRunningQueryBasedRule(runningQueryRoutingRuleSpec);
                    break;
                }
            }
        }
        catch (IOException e) {
            log.error(e, e.getMessage());
            throw new WebApplicationException(e);
        }
        return Response.ok().build();
    }

    @Path("/delete/cluster/{name}")
    @DELETE
    public Response deleteCluster(@PathParam("name") String name)
    {
        if (Strings.isNullOrEmpty(name)) {
            throw new WebApplicationException("name can not be null");
        }
        boolean success = multiClusterManager.deleteCluster(name);
        ((PullBasedPrestoClusterMonitor) statspuller).pullClusterStats();
        if (!success) {
            throw new WebApplicationException("CLUSTER NOT FOUND");
        }
        return Response.ok().build();
    }

    @Path("/delete/routingpolicy/{name}")
    @DELETE
    public Response deleteRoutingPolicy(@PathParam("name") String name)
    {
        this.routingManager.deleteRoutingPolicy(name);
        boolean success = multiClusterManager.deleteRoutingPolicy(name);
        if (!success) {
            throw new WebApplicationException("ROUTING POLICY NOT FOUND");
        }
        return Response.ok().build();
    }

    @Path("/delete/routingrule/{name}")
    @DELETE
    public Response deleteRoutingRule(@PathParam("name") String name)
    {
        this.routingManager.deleteRoutingRule(name);
        if (name.equalsIgnoreCase(RandomClusterRoutingRule.NAME) || name.equalsIgnoreCase(RoundRobinClusterRoutingRule.NAME)) {
            throw new WebApplicationException("Default rules can't be deleted.");
        }
        boolean success = multiClusterManager.deleteRoutingRule(name);
        if (!success) {
            throw new WebApplicationException("ROUTING RULE NOT FOUND");
        }
        return Response.ok().build();
    }

    @Path("/delete/routingpolicyselector/{name}")
    @DELETE
    public Response deleteRoutingPolicySelector(@PathParam("name") String name)
    {
        if (name.equalsIgnoreCase(RoutingPolicySpec.DEFAULT_POLICY_NAME)) {
            throw new WebApplicationException("Default routing policy can't be deleted.");
        }
        boolean success = multiClusterManager.deleteRoutingPolicySelector(name);
        if (!success) {
            throw new WebApplicationException("ROUTING POLICY SELECTOR NOT FOUND");
        }
        return Response.ok().build();
    }
}
