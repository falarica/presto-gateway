package io.prestosql.gateway.querymonitor;

import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;
import io.prestosql.gateway.ForGateway;
import io.prestosql.gateway.GatewayConfig;
import io.prestosql.gateway.persistence.JDBCConnectionManager;
import io.prestosql.gateway.persistence.dao.ExecutingQuery;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Pull based query monitor for expensive query.
 * Some expensive query can be observed for cancellation etc.
 */
@Singleton
public class PullBasedPrestoQueryExecutionMonitor
        implements QueryExecutionMonitor
{
    private final JDBCConnectionManager jdbcConnectionManager;
    private final HttpClient httpClient;

    private final ScheduledExecutorService executorService =
            Executors.newSingleThreadScheduledExecutor();
    private List<QueryExecutionObserver> queryExecutionObservers;
    private final GatewayConfig config;

    Logger log = Logger.get(PullBasedPrestoQueryExecutionMonitor.class);

    volatile boolean monitorActive;

    @Inject
    PullBasedPrestoQueryExecutionMonitor(@ForGateway HttpClient httpClient,
            JDBCConnectionManager connectionManager,
            GatewayConfig config)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.jdbcConnectionManager = requireNonNull(connectionManager, "jdbc conn manager is null");
        this.config = config;
    }

    //@PostConstruct
    @Override
    public void monitorQueryExecution(QueryExecutionContext context)
    {
        this.monitorActive = true;
    }

    @Override
    public void start()
    {
        /*executorService.submit(
                () -> {
                    while (monitorActive) {
                        try {
                            List<ClusterDetail> activeClusters =
                                    clusterManager.getAllClusters();
                            List<Future<SteerDClusterStats>> futures = new ArrayList<>();
                            for (ClusterDetail cluster : activeClusters) {
                                Future<SteerDClusterStats> call =
                                        executorService.submit(() -> getPrestoClusterStats(cluster));
                                futures.add(call);
                            }
                            List<SteerDClusterStats> stats = new ArrayList<>();
                            for (Future<SteerDClusterStats> clusterStatsFuture : futures) {
                                SteerDClusterStats steerDClusterStats = clusterStatsFuture.get();
                                stats.add(steerDClusterStats);
                            }

                            if (queryExecutionObservers != null) {
                                for (QueryExecutionObserver observer : queryExecutionObservers) {
                                    observer.observe(stats);
                                }
                            }
                        }
                        catch (Exception e) {
                            log.error("Error performing cluster monitor tasks %s", e);
                            e.printStackTrace();
                        }
                        try {
                            Thread.sleep(TimeUnit.MINUTES.toMillis(MONITOR_TASK_DELAY_MIN));
                        }
                        catch (Exception e) {
                            log.error("Error with monitor task %s", e);
                            e.printStackTrace();
                        }
                    }
                });*/
    }

    @Override
    public void stop()
    {
        this.monitorActive = false;
    }

    // TODO: Suranjan make it configurable
    @Override
    public void scheduleQueryHistoryCleanup()
    {
        executorService.scheduleWithFixedDelay(
                () -> {
                    log.info("Performing query history cleanup task");
                    try {
                        jdbcConnectionManager.open();
                        ExecutingQuery.delete(
                                "created < ?", System.currentTimeMillis() - config.getQueryHistoryCleanupInterval().toMillis());
                    }
                    finally {
                        jdbcConnectionManager.close();
                    }
                },
                1,
                1,
                TimeUnit.HOURS);
    }
}
