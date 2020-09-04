package io.prestosql.gateway.routing;

import io.prestosql.gateway.clustermonitor.SteerDClusterStats;
import io.prestosql.gateway.persistence.ClusterDetail;
import io.prestosql.server.ui.ClusterStatsResource;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestRoutingManager
{
    @Test
    public void testDefaultRule()
    {
        ArrayList<RoutingPolicySpec> policies = new ArrayList<>();
        ArrayList<RoutingRuleSpec> rules = new ArrayList<>();
        ClusterStruct cs = getTwoClusterStructs();
        TestMultiClusterManager clusterMgr = new TestMultiClusterManager(
                cs.clusterDetails,
                policies,
                rules);
        RoutingManager routingManager = new RoutingManager(clusterMgr, cs.testClusterStatsObserver);
        String uri = routingManager.providePrestoCluster(null, Optional.empty()).toString();
        Assertions.assertThat(uri.equals("xyz:8080") || uri.equals("abc:8080")).isTrue();
    }

    @Test
    public void testRunningQueryRule()
    {
        ArrayList<RoutingPolicySpec> policies = new ArrayList<>();
        Map<String, String> properties = new HashMap<>();
        List<String> ruleNames = new ArrayList<>();
        properties.put("numRunningQueries", "3");
        RoutingRuleSpec rule = new RunningQueryRoutingRule("3RunningQueryRule", properties);
        ruleNames.add("3RunningQueryRule");
        RoutingPolicySpec policy = new RoutingPolicySpec("3RunningQueryPolicy", ruleNames);

        ArrayList<RoutingRuleSpec> rules = new ArrayList<>();
        rules.add(rule);
        policies.add(policy);
        ClusterStruct cs = getTwoClusterStructs();
        TestMultiClusterManager clusterMgr = new TestMultiClusterManager(
                cs.clusterDetails,
                policies,
                rules);
        RoutingManager routingManager = new RoutingManager(clusterMgr, cs.testClusterStatsObserver);
        String uri = routingManager.providePrestoCluster(null, Optional.empty()).toString();
        Assertions.assertThat(uri.equals("xyz:8080")).isTrue();
    }

    @Test
    public void testQueuedQueryRule()
    {
        ArrayList<RoutingPolicySpec> policies = new ArrayList<>();
        Map<String, String> properties = new HashMap<>();
        List<String> ruleNames = new ArrayList<>();
        properties.put("numQueuedQueries", "3");
        RoutingRuleSpec rule = new QueuedQueryRoutingRule("3QueuedQueryRule", properties);
        ruleNames.add("3QueuedQueryRule");
        RoutingPolicySpec policy = new RoutingPolicySpec("3QueuedQueryPolicy", ruleNames);

        ArrayList<RoutingRuleSpec> rules = new ArrayList<>();
        rules.add(rule);
        policies.add(policy);
        ClusterStruct cs = getTwoClusterStructs();
        TestMultiClusterManager clusterMgr = new TestMultiClusterManager(
                cs.clusterDetails,
                policies,
                rules);
        RoutingManager routingManager = new RoutingManager(clusterMgr, cs.testClusterStatsObserver);
        String uri = routingManager.providePrestoCluster(null, Optional.empty()).toString();
        Assertions.assertThat(uri.equals("abc:8080")).isTrue();
    }

    @Test
    public void testQueuedRunningRuleNoneMatch()
    {
        ArrayList<RoutingPolicySpec> policies = new ArrayList<>();
        Map<String, String> properties1 = new HashMap<>();
        Map<String, String> properties2 = new HashMap<>();
        List<String> ruleNames = new ArrayList<>();
        properties1.put("numQueuedQueries", "1");
        properties2.put("numRunningQueries", "1");
        RoutingRuleSpec rule1 = new RunningQueryRoutingRule("5RunningQueryRule", properties2);
        RoutingRuleSpec rule2 = new QueuedQueryRoutingRule("5QueuedQueryRule", properties1);
        ruleNames.add("5QueuedQueryRule");
        ruleNames.add("5RunningQueryRule");
        RoutingPolicySpec policy = new RoutingPolicySpec("5QueuedQueryPolicy", ruleNames);

        ArrayList<RoutingRuleSpec> rules = new ArrayList<>();
        rules.add(rule1);
        rules.add(rule2);
        policies.add(policy);
        ClusterStruct cs = getTwoClusterStructs();
        TestMultiClusterManager clusterMgr = new TestMultiClusterManager(
                cs.clusterDetails,
                policies,
                rules);
        RoutingManager routingManager = new RoutingManager(clusterMgr, cs.testClusterStatsObserver);
        try {
            String uri = routingManager.providePrestoCluster(null, Optional.empty()).toString();
        }
        catch (IllegalStateException ie) {
            Assertions.assertThat(ie.getMessage()
                    .equalsIgnoreCase("Number of qualified cluster found zero")).isTrue();
        }
    }

    @Test
    public void testQueuedRunningRuleBothMatch()
    {
        ArrayList<RoutingPolicySpec> policies = new ArrayList<>();
        Map<String, String> properties1 = new HashMap<>();
        Map<String, String> properties2 = new HashMap<>();
        List<String> ruleNames = new ArrayList<>();
        properties1.put("numQueuedQueries", "5");
        properties2.put("numRunningQueries", "5");
        RoutingRuleSpec rule1 = new RunningQueryRoutingRule("1RunningQueryRule", properties2);
        RoutingRuleSpec rule2 = new QueuedQueryRoutingRule("1QueuedQueryRule", properties1);
        ruleNames.add("1QueuedQueryRule");
        ruleNames.add("1RunningQueryRule");
        RoutingPolicySpec policy = new RoutingPolicySpec("1QueuedQueryPolicy", ruleNames);

        ArrayList<RoutingRuleSpec> rules = new ArrayList<>();
        rules.add(rule1);
        rules.add(rule2);
        policies.add(policy);
        ClusterStruct cs = getTwoClusterStructs();
        TestMultiClusterManager clusterMgr = new TestMultiClusterManager(
                cs.clusterDetails,
                policies,
                rules);
        RoutingManager routingManager = new RoutingManager(clusterMgr, cs.testClusterStatsObserver);
        String uri = routingManager.providePrestoCluster(null, Optional.empty()).toString();
        Assertions.assertThat(uri.equals("xyz:8080")).isTrue();
    }

    @Test
    public void testQueuedRuleFailure()
    {
        ArrayList<RoutingPolicySpec> policies = new ArrayList<>();
        Map<String, String> properties1 = new HashMap<>();
        Map<String, String> properties2 = new HashMap<>();
        List<String> ruleNames = new ArrayList<>();
        properties1.put("numQueuedQueries", "3");
        properties2.put("numRunningQueries", "5");
        RoutingRuleSpec rule1 = new RunningQueryRoutingRule("1RunningQueryRule", properties2);
        RoutingRuleSpec rule2 = new QueuedQueryRoutingRule("3QueuedQueryRule", properties1);
        ruleNames.add("3QueuedQueryRule");
        ruleNames.add("1RunningQueryRule");
        RoutingPolicySpec policy = new RoutingPolicySpec("1QueuedQueryPolicy", ruleNames);

        ArrayList<RoutingRuleSpec> rules = new ArrayList<>();
        rules.add(rule1);
        rules.add(rule2);
        policies.add(policy);
        ClusterStruct cs = getTwoClusterStructs();
        TestMultiClusterManager clusterMgr = new TestMultiClusterManager(
                cs.clusterDetails,
                policies,
                rules);
        RoutingManager routingManager = new RoutingManager(clusterMgr, cs.testClusterStatsObserver);
        String uri = routingManager.providePrestoCluster(null, Optional.empty()).toString();
        System.out.println(uri.toString());
        //TODO:Fix code then enable following: xyz should not match the queued query rule and hence abc should be returned.

        Assertions.assertThat(uri.equals("abc:8080")).isTrue();
    }

    @Test
    public void testRunningRuleFailure()
    {
        ArrayList<RoutingPolicySpec> policies = new ArrayList<>();
        Map<String, String> properties1 = new HashMap<>();
        Map<String, String> properties2 = new HashMap<>();
        List<String> ruleNames = new ArrayList<>();
        properties1.put("numQueuedQueries", "5");
        properties2.put("numRunningQueries", "3");
        RoutingRuleSpec rule1 = new RunningQueryRoutingRule("3RunningQueryRule", properties2);
        RoutingRuleSpec rule2 = new QueuedQueryRoutingRule("1QueuedQueryRule", properties1);
        ruleNames.add("1QueuedQueryRule");
        ruleNames.add("3RunningQueryRule");
        RoutingPolicySpec policy = new RoutingPolicySpec("3QueuedQueryPolicy", ruleNames);

        ArrayList<RoutingRuleSpec> rules = new ArrayList<>();
        rules.add(rule1);
        rules.add(rule2);
        policies.add(policy);
        ClusterStruct cs = getTwoClusterStructs();
        TestMultiClusterManager clusterMgr = new TestMultiClusterManager(
                cs.clusterDetails,
                policies,
                rules);
        RoutingManager routingManager = new RoutingManager(clusterMgr, cs.testClusterStatsObserver);
        String uri = routingManager.providePrestoCluster(null, Optional.empty()).toString();
        Assertions.assertThat(uri.equals("xyz:8080")).isTrue();
    }

    @Test
    public void testRoundRobinRule2()
    {
        ArrayList<RoutingPolicySpec> policies = new ArrayList<>();
        List<String> ruleNames = new ArrayList<>();
        RoutingRule rule = new RoundRobinClusterRoutingRule();
        ruleNames.add(RoundRobinClusterRoutingRule.NAME);
        RoutingPolicySpec policy = new RoutingPolicySpec("RoundRobin", ruleNames);

        ArrayList<RoutingRuleSpec> rules = new ArrayList<>();
        policies.add(policy);
        ClusterStruct cs = getNClusterStructs(100);
        TestMultiClusterManager clusterMgr = new TestMultiClusterManager(
                cs.clusterDetails,
                policies,
                rules);
        RoutingManager routingManager = new RoutingManager(clusterMgr, cs.testClusterStatsObserver);
        routingManager.addRoutingRule(new RoundRobinClusterRoutingRule());
        for (int i = 0; i < 100; i++) {
            String uri1 = routingManager.providePrestoCluster(null, Optional.empty()).toString();
            System.out.println("i: " + i + " " + uri1.toString());
            //TODO:Fix code then enable following: round robin should return abc in second time

            Assertions.assertThat(uri1.equals("xyz" + i + ":8080")).isTrue();
        }
    }

    @Test
    public void testRoundRobinRuleMultithreaded()
            throws InterruptedException
    {
        ArrayList<RoutingPolicySpec> policies = new ArrayList<>();
        List<String> ruleNames = new ArrayList<>();
        ruleNames.add(RoundRobinClusterRoutingRule.NAME);
        RoutingPolicySpec policy = new RoutingPolicySpec("RoundRobin", ruleNames);

        ArrayList<RoutingRuleSpec> rules = new ArrayList<>();
        policies.add(policy);
        ClusterStruct cs = getTwoClusterStructs();
        TestMultiClusterManager clusterMgr = new TestMultiClusterManager(
                cs.clusterDetails,
                policies,
                rules);
        RoutingManager routingManager = new RoutingManager(clusterMgr, cs.testClusterStatsObserver);
        routingManager.addRoutingRule(new RoundRobinClusterRoutingRule());
        ExecutorService service = Executors.newFixedThreadPool(10);

        BlockingQueue q = new LinkedBlockingQueue();
        for (int i = 0; i < 10; i++) {
            service.execute(() -> {
                for (int i1 = 0; i1 < 100; i1++) {
                    String uri1 = routingManager.providePrestoCluster(null, Optional.empty()).toString();
                    q.add("Threadid " + Thread.currentThread().getId() + " : " + uri1.toString());
                }
            });
        }
        service.shutdown();
        service.awaitTermination(100L, TimeUnit.SECONDS);
        //q.stream().forEach(i -> System.out.println(i));
        Assertions.assertThat(q.size() == 1000);
    }

    @Test
    public void testRoundRobinRule()
    {
        ArrayList<RoutingPolicySpec> policies = new ArrayList<>();
        List<String> ruleNames = new ArrayList<>();
        RoutingRule rule = new RoundRobinClusterRoutingRule();
        ruleNames.add(RoundRobinClusterRoutingRule.NAME);
        RoutingPolicySpec policy = new RoutingPolicySpec("RoundRobin", ruleNames);

        ArrayList<RoutingRuleSpec> rules = new ArrayList<>();
        policies.add(policy);
        ClusterStruct cs = getTwoClusterStructs();
        TestMultiClusterManager clusterMgr = new TestMultiClusterManager(
                cs.clusterDetails,
                policies,
                rules);
        RoutingManager routingManager = new RoutingManager(clusterMgr, cs.testClusterStatsObserver);
        routingManager.addRoutingRule(new RoundRobinClusterRoutingRule());
        String uri = routingManager.providePrestoCluster(null, Optional.empty()).toString();
        Assertions.assertThat(uri.equals("abc:8080")).isTrue();
        for (int i = 0; i < 100; i++) {
            String uri1 = routingManager.providePrestoCluster(null, Optional.empty()).toString();
            //System.out.println(uri1.toString());
            if (i % 2 == 0) {
                Assertions.assertThat(uri1.equals("xyz:8080")).isTrue();
            }
            else {
                Assertions.assertThat(uri1.equals("abc:8080")).isTrue();
            }
        }
    }

    @Test
    public void testQueuedQueryAndRoundRobinRule()
    {
        ArrayList<RoutingPolicySpec> policies = new ArrayList<>();
        Map<String, String> properties = new HashMap<>();
        List<String> ruleNames = new ArrayList<>();
        properties.put("numQueuedQueries", "3");
        RoutingRuleSpec rule = new QueuedQueryRoutingRule("3QueuedQueryRule", properties);
        ruleNames.add("3QueuedQueryRule");
        ruleNames.add(RoundRobinClusterRoutingRule.NAME);
        RoutingPolicySpec policy = new RoutingPolicySpec("3QueuedQueryPolicy", ruleNames);

        ArrayList<RoutingRuleSpec> rules = new ArrayList<>();
        rules.add(rule);
        policies.add(policy);
        ClusterStruct cs = getTwoClusterStructs();
        TestMultiClusterManager clusterMgr = new TestMultiClusterManager(
                cs.clusterDetails,
                policies,
                rules);
        RoutingManager routingManager = new RoutingManager(clusterMgr, cs.testClusterStatsObserver);
        routingManager.addRoutingRule(new RoundRobinClusterRoutingRule());
        String uri = routingManager.providePrestoCluster(null, Optional.empty()).toString();
        System.out.println(uri.toString());
        //TODO:Fix code then enable following: since queued query rule is applied, only abc should be selected.
        // irrespective of round robin, this should always return abc

        Assertions.assertThat(uri.equals("abc:8080")).isTrue();
        String uri1 = routingManager.providePrestoCluster(null, Optional.empty()).toString();
        System.out.println(uri1.toString());
        Assertions.assertThat(uri1.equals("abc:8080")).isTrue();
    }

    private ClusterStruct getNClusterStructs(int n)
    {
        ClusterStatsResource.ClusterStats clusterStatsxyz = new ClusterStatsResource.ClusterStats(2, 0, 4,
                1, 1, 1, 1,
                1, 1, 1, 1);
        ArrayList<ClusterDetail> clusterDetails = new ArrayList<>();
        ArrayList<SteerDClusterStats> steerdClusterStats = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            clusterDetails.add(new ClusterDetail(String.valueOf(i),
                    "mumbai",
                    "xyz" + i + ":8080",
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    true,
                    Optional.empty()));
            steerdClusterStats.add(new SteerDClusterStats(clusterStatsxyz, true, String.valueOf(i), "xyz" + i + ":8080", "mumbai"));
        }

        TestClusterStatsObserver testClusterStatsObserver = new TestClusterStatsObserver(steerdClusterStats);
        ClusterStruct clusterStruct = new ClusterStruct();
        clusterStruct.clusterDetails = clusterDetails;
        clusterStruct.testClusterStatsObserver = testClusterStatsObserver;
        return clusterStruct;
    }

    private ClusterStruct getTwoClusterStructs()
    {
        ClusterStatsResource.ClusterStats clusterStatsabc = new ClusterStatsResource.ClusterStats(4, 0, 2,
                1, 1, 1, 1,
                1, 1, 1, 1);
        ClusterStatsResource.ClusterStats clusterStatsxyz = new ClusterStatsResource.ClusterStats(2, 0, 4,
                1, 1, 1, 1,
                1, 1, 1, 1);
        ArrayList<ClusterDetail> clusterDetails = new ArrayList<>();
        clusterDetails.add(new ClusterDetail("xyz",
                "mumbai",
                "xyz:8080",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty()));
        clusterDetails.add(new ClusterDetail("abc",
                "mumbai",
                "abc:8080",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty()));

        ArrayList<SteerDClusterStats> steerdClusterStats = new ArrayList<>();
        steerdClusterStats.add(new SteerDClusterStats(clusterStatsxyz, true, "xyz", "xyz:8080", "mumbai"));
        steerdClusterStats.add(new SteerDClusterStats(clusterStatsabc, true, "abc", "abc:8080", "mumbai"));
        TestClusterStatsObserver testClusterStatsObserver = new TestClusterStatsObserver(steerdClusterStats);
        ClusterStruct clusterStruct = new ClusterStruct();
        clusterStruct.clusterDetails = clusterDetails;
        clusterStruct.testClusterStatsObserver = testClusterStatsObserver;
        return clusterStruct;
    }

    static class ClusterStruct
    {
        public TestClusterStatsObserver testClusterStatsObserver;
        public ArrayList<ClusterDetail> clusterDetails;
    }
}
