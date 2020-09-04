package io.prestosql.gateway.routing;

import io.airlift.json.JsonCodec;
import io.prestosql.execution.QueryInfo;
import io.prestosql.gateway.MultiClusterManager;
import io.prestosql.gateway.persistence.ClusterDetail;
import io.prestosql.gateway.persistence.QueryDetails2;
import io.prestosql.gateway.persistence.dao.Cluster;
import io.prestosql.gateway.persistence.dao.RoutingPolicy;

import java.util.List;

public class TestMultiClusterManager
        implements MultiClusterManager
{
    private final List<ClusterDetail> clusterDetails;
    private final List<RoutingPolicySpec> policies;
    private final List<RoutingRuleSpec> rules;

    public TestMultiClusterManager(
            List<ClusterDetail> clusterDetails,
            List<RoutingPolicySpec> policies,
            List<RoutingRuleSpec> rules)
    {
        this.clusterDetails = clusterDetails;
        this.policies = policies;
        this.rules = rules;
    }

    @Override
    public List<ClusterDetail> getAllClusters()
    {
        return this.clusterDetails;
    }

    @Override
    public List<ClusterDetail> getAllActiveClusters()
    {
        return this.clusterDetails;
    }

    @Override
    public List<ClusterDetail> getAllActiveClusterStats()
    {
        return this.clusterDetails;
    }

    @Override
    public List<ClusterDetail> getAllClusters(String location)
    {
        throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public Cluster addCluster(ClusterDetail info)
    {
        throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public boolean deleteCluster(String name)
    {
        throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public void addRoutingRule(RoutingRuleSpec ruleSpec)
    {
        //throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public void addRoutingPolicy(RoutingPolicySpec policySpec)
    {
        // throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public void addRoutingPolicySelector(RoutingPolicySelectorSpec selectorSpec)
    {
        throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public void updatePolicy(RoutingPolicySpec policySpec)
    {
        throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public void updateQueuedQueryBasedRule(QueuedQueryRoutingRule queuedQueryRoutingRule)
    {
        throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public void updateRunningQueryBasedRule(RunningQueryRoutingRule runningQueryBasedRule)
    {
        throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public List<RoutingPolicySpec> getAllRoutingPolicies()
    {
        return this.policies;
    }

    @Override
    public List<RoutingRuleSpec> getAllRoutingRules()
    {
        return this.rules;
    }

    @Override
    public List<RoutingPolicySelectorSpec> getAllRoutingPolicySelectors()
    {
        throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public boolean deleteRoutingPolicy(String name)
    {
        throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public boolean deleteRoutingRule(String name)
    {
        throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public boolean deleteRoutingPolicySelector(String name)
    {
        throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public List<RoutingPolicy> loadAllRulesFromDB()
    {
        throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public ClusterDetail updateCluster(ClusterDetail into)
    {
        throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public List<QueryDetails2> getAllQueries()
    {
        throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public void setQueryInfoCodec(JsonCodec<QueryInfo> codec)
    {
        throw new UnsupportedOperationException("not supported in TestMultiClusterManager");
    }

    @Override
    public List<RoutingRuleSpec> loadAllRoutingRules()
    {
        return this.rules;
    }

    @Override
    public List<RoutingPolicySpec> loadAllRoutingPolicy()
    {
        return this.policies;
    }
}
