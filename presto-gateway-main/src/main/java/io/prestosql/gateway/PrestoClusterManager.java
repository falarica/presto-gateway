package io.prestosql.gateway;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.prestosql.execution.QueryInfo;
import io.prestosql.gateway.persistence.ClusterDetail;
import io.prestosql.gateway.persistence.JDBCConnectionManager;
import io.prestosql.gateway.persistence.QueryDetails2;
import io.prestosql.gateway.persistence.dao.Cluster;
import io.prestosql.gateway.persistence.dao.ExecutingQuery;
import io.prestosql.gateway.persistence.dao.RoutingPolicy;
import io.prestosql.gateway.persistence.dao.RoutingPolicySelector;
import io.prestosql.gateway.persistence.dao.RoutingRuleModel;
import io.prestosql.gateway.routing.QueuedQueryRoutingRule;
import io.prestosql.gateway.routing.RoutingPolicySelectorSpec;
import io.prestosql.gateway.routing.RoutingPolicySpec;
import io.prestosql.gateway.routing.RoutingRuleSpec;
import io.prestosql.gateway.routing.RunningQueryRoutingRule;

import java.io.IOException;
import java.util.List;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newFixedThreadPool;

@Singleton
public class PrestoClusterManager
        implements MultiClusterManager
{
    protected final JDBCConnectionManager connectionManager;
    protected final NodeInfo nodeInfo;
    private final Logger log = Logger.get(PrestoClusterManager.class);
    protected final ListeningExecutorService executor = listeningDecorator(
            newFixedThreadPool(Runtime.getRuntime().availableProcessors(), threadsNamed("cluster-mgr-%s")));
    private JsonCodec<QueryInfo> queryInfoCodec;

    private final GatewayConfig config;

    @Inject
    public PrestoClusterManager(JDBCConnectionManager mgr,
            GatewayConfig config,
            NodeInfo nodeInfo)
    {
        this.connectionManager = mgr;
        this.nodeInfo = nodeInfo;
        this.config = config;
    }

    @JsonCreator
    @Override
    public List<ClusterDetail> getAllClusters()
    {
        try {
            connectionManager.open();
            List<Cluster> clusters = Cluster.findAll();
            return Cluster.upcast(clusters);
        }
        finally {
            connectionManager.close();
        }
    }

    @JsonCreator
    @Override
    public List<ClusterDetail> getAllActiveClusterStats()
    {
        try {
            connectionManager.open();
            List<Cluster> clusters = Cluster.findAll();
            return Cluster.upcast(clusters);
        }
        finally {
            connectionManager.close();
        }
    }

    @Override
    public List<ClusterDetail> getAllActiveClusters()
    {
        try {
            connectionManager.open();
            List<Cluster> clusters = Cluster.findBySQL("select clusters.* from clusters where active=1");
            return Cluster.upcast(clusters);
        }
        finally {
            connectionManager.close();
        }
    }

    @JsonCreator
    @Override
    public List<ClusterDetail> getAllClusters(String location)
    {
        try {
            connectionManager.open();
            List<Cluster> clusters = Cluster.findBySQL("select clusters.* from clusters where location=?", location);
            return Cluster.upcast(clusters);
        }
        finally {
            connectionManager.close();
        }
    }

    @Override
    public synchronized Cluster addCluster(ClusterDetail info)
    {
        Cluster c = null;
        try {
            connectionManager.open();
            c = Cluster.create(info);
        }
        finally {
            connectionManager.close();
        }
        return c;
    }

    @Override
    public synchronized boolean deleteCluster(String name)
    {
        try {
            connectionManager.open();
            Cluster model = Cluster.findFirst("name = ?", name);
            if (model != null) {
                return model.delete();
            }
        }
        finally {
            connectionManager.close();
        }
        return Boolean.FALSE;
    }

    @Override
    public synchronized void updatePolicy(RoutingPolicySpec policySpec)
    {
        try {
            connectionManager.open();
            RoutingPolicy model = RoutingPolicy.findFirst("name = ?", policySpec.getName());
            if (model != null) {
                RoutingPolicy.update(model, policySpec);
            }
        }
        catch (IOException e) {
            log.warn(e, "Caught exception while processing request");
            throw new RuntimeException("Couldn't update routing policy.");
        }
        finally {
            connectionManager.close();
        }
    }

    @Override
    public synchronized void updateQueuedQueryBasedRule(QueuedQueryRoutingRule queuedQueryRoutingRule)
    {
        try {
            connectionManager.open();
            RoutingRuleModel model = RoutingRuleModel.findFirst("name = ?", queuedQueryRoutingRule.getName());
            RoutingRuleModel.update(model, queuedQueryRoutingRule);
        }
        catch (IOException e) {
            log.warn(e, "Caught exception while processing request.");
            throw new RuntimeException("Couldn't update routing rule.");
        }
        finally {
            connectionManager.close();
        }
    }

    @Override
    public synchronized void updateRunningQueryBasedRule(RunningQueryRoutingRule runningQueryBasedRule)
    {
        try {
            connectionManager.open();
            RoutingRuleModel model = RoutingRuleModel.findFirst("name = ?", runningQueryBasedRule.getName());
            RoutingRuleModel.update(model, runningQueryBasedRule);
        }
        catch (IOException e) {
            log.warn(e, "Caught exception while processing request.");
            throw new RuntimeException("Couldn't update routing rule.");
        }
        finally {
            connectionManager.close();
        }
    }

    @Override
    public synchronized void addRoutingRule(RoutingRuleSpec ruleSpec)
    {
        try {
            connectionManager.open();
            RoutingRuleModel.create(ruleSpec);
        }
        catch (JsonProcessingException e) {
            // already parsed  so we shouldn't get this exception
        }
        finally {
            connectionManager.close();
        }
    }

    @Override
    public synchronized void addRoutingPolicy(RoutingPolicySpec policySpec)
    {
        try {
            connectionManager.open();
            RoutingPolicy.create(policySpec);
        }
        catch (JsonProcessingException e) {
            // already parsed  so we shouldn't get this exception
        }
        finally {
            connectionManager.close();
        }
    }

    @Override
    public synchronized void addRoutingPolicySelector(RoutingPolicySelectorSpec selectorSpec)
    {
        try {
            connectionManager.open();
            RoutingPolicySelector.create(selectorSpec);
        }
        finally {
            connectionManager.close();
        }
    }

    @Override
    public List<RoutingPolicySpec> getAllRoutingPolicies()
    {
        try {
            connectionManager.open();
            List<RoutingPolicy> policies = RoutingPolicy.findAll();
            return RoutingPolicy.upcast(policies);
        }
        finally {
            connectionManager.close();
        }
    }

    @Override
    public List<RoutingRuleSpec> getAllRoutingRules()
    {
        try {
            connectionManager.open();
            List<RoutingRuleModel> rules = RoutingRuleModel.findAll();
            return RoutingRuleModel.upcast(rules);
        }
        finally {
            connectionManager.close();
        }
    }

    @Override
    public List<RoutingPolicySelectorSpec> getAllRoutingPolicySelectors()
    {
        try {
            connectionManager.open();
            List<RoutingPolicySelector> policySelectors = RoutingPolicySelector.findAll();
            return RoutingPolicySelector.upcast(policySelectors);
        }
        finally {
            connectionManager.close();
        }
    }

    @Override
    public synchronized boolean deleteRoutingPolicy(String name)
    {
        try {
            connectionManager.open();
            RoutingPolicy model = RoutingPolicy.findFirst("name = ?", name);
            if (model != null) {
                return model.delete();
            }
        }
        finally {
            connectionManager.close();
        }
        return Boolean.FALSE;
    }

    @Override
    public synchronized boolean deleteRoutingRule(String name)
    {
        try {
            connectionManager.open();
            RoutingRuleModel model = RoutingRuleModel.findFirst("name = ?", name);
            if (model != null) {
                return model.delete();
            }
        }
        finally {
            connectionManager.close();
        }
        return Boolean.FALSE;
    }

    @Override
    public synchronized boolean deleteRoutingPolicySelector(String name)
    {
        try {
            connectionManager.open();
            RoutingPolicySelector model = RoutingPolicySelector.findFirst("name = ?", name);
            if (model != null) {
                return model.delete();
            }
        }
        finally {
            connectionManager.close();
        }
        return Boolean.FALSE;
    }

    @Override
    public synchronized List<RoutingPolicy> loadAllRulesFromDB()
    {
        try {
            connectionManager.open();
            List<RoutingPolicy> policies = RoutingPolicy.findAll();
            return policies;
        }
        finally {
            connectionManager.close();
        }
    }

    @Override
    public synchronized ClusterDetail updateCluster(ClusterDetail info)
    {
        try {
            connectionManager.open();
            Cluster model = Cluster.findFirst("name = ?", info.getName());
            if (model == null) {
                Cluster.create(model, info);
            }
            else {
                Cluster.update(model, info);
            }
        }
        finally {
            connectionManager.close();
        }
        return info;
    }

    @Override
    public List<QueryDetails2> getAllQueries()
    {
        try {
            connectionManager.open();
            List<ExecutingQuery> qStats = ExecutingQuery.findAll().orderBy("created desc")
                    .limit(config.getUiMaxQueryInfoListSize());

            return ExecutingQuery.castToQueryDetails(qStats);
        }
        finally {
            connectionManager.close();
        }
    }

    public void setQueryInfoCodec(JsonCodec<QueryInfo> codec)
    {
        this.queryInfoCodec = codec;
    }

    @Override
    public List<RoutingRuleSpec> loadAllRoutingRules()
    {
        try {
            connectionManager.open();
            List<RoutingRuleModel> rules = RoutingRuleModel.findAll();
            return RoutingRuleModel.upcast(rules);
        }
        finally {
            connectionManager.close();
        }
    }

    @Override
    public List<RoutingPolicySpec> loadAllRoutingPolicy()
    {
        try {
            connectionManager.open();
            List<RoutingPolicy> policies = RoutingPolicy.findAll();
            return RoutingPolicy.upcast(policies);
        }
        finally {
            connectionManager.close();
        }
    }
}
