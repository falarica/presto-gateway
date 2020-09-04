package io.prestosql.gateway.clustermonitor;

public interface ClusterMonitor
{
    void monitorClusters();

    void registerObserver(ClusterStatsObserver ob);

    void unregisterObserver(ClusterStatsObserver ob);

    void start();

    void stop();
}
