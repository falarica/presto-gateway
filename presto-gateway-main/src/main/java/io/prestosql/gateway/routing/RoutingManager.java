package io.prestosql.gateway.routing;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.gateway.MultiClusterManager;
import io.prestosql.gateway.clustermonitor.ClusterStatsObserver;
import io.prestosql.gateway.clustermonitor.SteerDClusterStats;
import io.prestosql.gateway.persistence.ClusterDetail;
import io.prestosql.server.GatewayRequestSessionContext;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.HttpMethod;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * It will have routing policy vs selectors so that
 * a particular policy could be appl
 */
@Singleton
public class RoutingManager
{
    protected final LoadingCache<String, String> queryIdBackendCache;
    protected ExecutorService executorService = Executors.newFixedThreadPool(5);
    protected MultiClusterManager multiClusterManager;
    Logger log = Logger.get(RoutingManager.class);
    protected Map<String, RoutingRule> rules;
    List<RoutingPolicySpec> policies;
    private final ClusterStatsObserver clusterStatsObserver;

    private Map<String, String> txIdToURLMap;

    @Inject
    public RoutingManager(MultiClusterManager clusterMgr,
            ClusterStatsObserver observer)
    {
        this.multiClusterManager = clusterMgr;
        rules = new ConcurrentHashMap<>();
        this.policies = new ArrayList<>();
        // we need to fetch from the db all the rules that had been created.
        loadAllRulesFromDB();
        // we also need to fetch from the db all the policies that had been created.
        this.clusterStatsObserver = observer;
        this.txIdToURLMap = new ConcurrentHashMap<>();
        queryIdBackendCache =
                CacheBuilder.newBuilder()
                        .maximumSize(10000)
                        .expireAfterAccess(30, TimeUnit.MINUTES)
                        .build(
                                new CacheLoader<>()
                                {
                                    @Override
                                    public String load(String queryId)
                                    {
                                        return findBackendForUnknownQueryId(queryId);
                                    }
                                });
    }

    protected synchronized void loadAllRulesFromDB()
    {
        // First check if Random and Roundrobin are there or not
        List<RoutingRuleSpec> rules = this.multiClusterManager.loadAllRoutingRules();
        if (rules.isEmpty()) {
            this.multiClusterManager.addRoutingRule(new RandomClusterRoutingRule());
            this.multiClusterManager.addRoutingRule(new RoundRobinClusterRoutingRule());
            this.rules.put(RandomClusterRoutingRule.NAME, new RandomClusterRoutingRule());
            this.rules.put(RoundRobinClusterRoutingRule.NAME, new RoundRobinClusterRoutingRule());
        }
        rules.stream().forEach(rule -> this.rules.put(rule.getName(), rule));
        this.policies = this.multiClusterManager.loadAllRoutingPolicy();
        if (policies.size() == 0) {
            String[] names = {RandomClusterRoutingRule.NAME};
            RoutingPolicySpec defaultRoutingPolicy = new RoutingPolicySpec(RoutingPolicySpec.DEFAULT_POLICY_NAME, Arrays.asList(names));
            this.multiClusterManager.addRoutingPolicy(defaultRoutingPolicy);
            this.policies.add(defaultRoutingPolicy);
        }
        this.policies = this.multiClusterManager.loadAllRoutingPolicy();
        if (this.policies.size() == 0) {
            String[] names = {RandomClusterRoutingRule.NAME};
            RoutingPolicySpec defaultRoutingPolicy = new RoutingPolicySpec(RoutingPolicySpec.DEFAULT_POLICY_NAME, Arrays.asList(names));
            this.policies.add(defaultRoutingPolicy);
        }
    }

    public void setBackendForQueryId(String queryId, String backend)
    {
        queryIdBackendCache.put(queryId, backend);
    }

    protected List<SteerDClusterStats> createMultiClusterContext()
    {
        List<SteerDClusterStats> stats = clusterStatsObserver.getStats();
        if (stats.size() == 0) {
            throw new IllegalStateException("Number of active cluster found zero");
        }
        return stats;
    }

    /**
     * Performs routing to a cluster based an applied rules.
     *
     * @return
     */
    public URI providePrestoCluster(GatewayRequestSessionContext schedulingQueryContext, Optional<Session> session)
    {
        // if txId is present in the session
        if (schedulingQueryContext != null && schedulingQueryContext.getTransactionId().isPresent()) {
            String routedUrl = this.txIdToURLMap.get(schedulingQueryContext.getTransactionId().get().toString());
            return URI.create(routedUrl);
        }
        List<SteerDClusterStats> clusterStats = createMultiClusterContext();
        // Find out the policy for the user and group
        // and apply that policy.
        // TODO: Suranjan get the policy for the user/group and apply he policy.

        // In OSS there is only one policy.
        List<SteerDClusterStats> clusters = clusterStats;
        for (RoutingPolicySpec policy : this.policies) {
            for (String ruleName : policy.getRoutingRules()) {
                RoutingRule rule = this.rules.get(ruleName);
                clusters = rule.apply(schedulingQueryContext, clusters, session);
            }
        }
        Optional<String> url = clusters.stream().map(c -> c.getClusterUrl()).findFirst();
        if (url.isEmpty()) {
            throw new IllegalStateException("Number of qualified cluster found zero");
        }
        return URI.create(url.get());
    }

    /**
     * Performs cache look up, if a backend not found, it checks with all cluster and tries to find
     * out which backend has info about given query id.
     *
     * @param queryId
     * @return
     */
    public String findBackendForQueryId(String queryId)
    {
        String backendAddress = null;
        try {
            backendAddress = queryIdBackendCache.get(queryId);
        }
        catch (ExecutionException e) {
            log.error(e, "Exception while loading queryId from cache");
        }
        return backendAddress;
    }

    public void addTxIdToRoutedCluster(String txId, String url)
    {
        this.txIdToURLMap.put(txId, url);
    }

    public void removeTxIdToRoutedCluster(String txId)
    {
        this.txIdToURLMap.remove(txId);
    }

    /**
     * This tries to find out which backend may have info about given query id. If not found returns
     * the first healthy backend.
     *
     * @param queryId
     * @return
     */
    protected String findBackendForUnknownQueryId(String queryId)
    {
        List<ClusterDetail> clusters = multiClusterManager.getAllClusters();

        Map<String, Future<Integer>> responseCodes = new HashMap<>();
        try {
            for (ClusterDetail cluster : clusters) {
                String target = cluster.getClusterUrl() + "/v1/query/" + queryId;

                Future<Integer> call =
                        executorService.submit(
                                () -> {
                                    URL url = new URL(target);
                                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                                    conn.setConnectTimeout((int) TimeUnit.SECONDS.toMillis(5));
                                    conn.setReadTimeout((int) TimeUnit.SECONDS.toMillis(5));
                                    conn.setRequestMethod(HttpMethod.HEAD);
                                    return conn.getResponseCode();
                                });
                responseCodes.put(cluster.getClusterUrl(), call);
            }
            for (Map.Entry<String, Future<Integer>> entry : responseCodes.entrySet()) {
                if (entry.getValue().isDone()) {
                    int responseCode = entry.getValue().get();
                    if (responseCode == 200) {
                        log.info("Found query %s on cluster %s", queryId, entry.getKey());
                        setBackendForQueryId(queryId, entry.getKey());
                        return entry.getKey();
                    }
                }
            }
        }
        catch (Exception e) {
            log.warn(e, "Query id %s not found", queryId);
        }
        // Fallback on first active backend if queryId mapping not found.
        return multiClusterManager.getAllClusters().get(0).getClusterUrl();
    }

    public synchronized void addRoutingRule(RoutingRuleSpec rule)
    {
        this.multiClusterManager.addRoutingRule(rule);
        this.rules.put(rule.getName(), rule);
    }

    public synchronized void addRoutingPolicy(RoutingPolicySpec spec)
    {
        for (String name : spec.getRoutingRules()) {
            if (!rules.containsKey(name)) {
                throw new IllegalStateException("The added rule in RoutingPolicy Not Found.");
            }
        }
        this.multiClusterManager.addRoutingPolicy(spec);
        this.policies.add(spec);
    }

    public void deleteRoutingRule(String name)
    {
        for (RoutingPolicySpec policy : this.policies) {
            if (policy.getRoutingRules().contains(name)) {
                throw new IllegalStateException("Can't delete the RoutingRule as it is being used by RoutingPopicy :" + policy.getName());
            }
        }
        this.rules.remove(name);
    }

    public void deleteRoutingPolicy(String name)
    {
        if (this.policies.size() == 1 && name.equals("_DEFAULT_ROUTING_POLICY_")) {
            // can't remove default policy.
            throw new IllegalStateException("Can't remove the default routing policy.");
        }
        else {
            ListIterator<RoutingPolicySpec> itr = this.policies.listIterator();
            while (itr.hasNext()) {
                if (itr.next().getName().equals(name)) {
                    itr.remove();
                    break;
                }
            }
        }
    }

    public void updatePolicy(RoutingPolicySpec policy)
    {
        Iterator<RoutingPolicySpec> itr = this.policies.listIterator();
        boolean found = false;
        while (itr.hasNext()) {
            if (itr.next().getName().equals(policy.getName())) {
                itr.remove();
                found = true;
            }
        }
        if (found) {
            this.policies.add(policy);
        }
    }
}
