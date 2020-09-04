package io.prestosql.gateway;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.http.client.HttpStatus;
import io.airlift.log.Logger;
import io.prestosql.gateway.clustermonitor.PullBasedPrestoClusterMonitor;
import io.prestosql.gateway.persistence.ClusterDetail;
import io.prestosql.gateway.persistence.JDBCConnectionManager;
import io.prestosql.gateway.persistence.QueryDetail;
import io.prestosql.gateway.persistence.QueryDetails2;
import io.prestosql.gateway.persistence.dao.ExecutingQuery;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.net.ssl.HttpsURLConnection;
import javax.ws.rs.HttpMethod;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.prestosql.gateway.clustermonitor.PullBasedPrestoClusterMonitor.CLUSTER_CONNECT_TIMEOUT_SECONDS;
import static io.prestosql.gateway.clustermonitor.PullBasedPrestoClusterMonitor.LOGIN_PATH;
import static io.prestosql.gateway.clustermonitor.PullBasedPrestoClusterMonitor.setSSLcontext;
import static java.util.Objects.requireNonNull;

@Singleton
public final class QueryHistoryManager
{
    private JDBCConnectionManager connectionManager;
    private MultiClusterManager mgr;
    private final GatewayConfig config;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    Logger log = Logger.get(QueryHistoryManager.class);

    private static final String UI_QUERY_STATS_PATH = "/ui/api/query/";
    private static final String UI_QUERY_STATS_PATH_PRESTODB = "/v1/query/";

    @Inject
    public QueryHistoryManager(JDBCConnectionManager mgr, MultiClusterManager clusterManager, GatewayConfig config)
    {
        CookieHandler.setDefault(new CookieManager());
        this.connectionManager = requireNonNull(mgr, "jdbc connection manager is null");
        this.mgr = clusterManager;
        this.config = config;
    }

    public synchronized void submitRunningQueryDetail(QueryDetail queryDetail)
    {
        try {
            connectionManager.open();
            ExecutingQuery.create(queryDetail);
            log.debug("created Executing query for %s", queryDetail);
        }
        finally {
            connectionManager.close();
        }
    }

    // This will remove from the running query table and add to query stats table
    // Fetching stats could be a costly operation for smaller query.
    // we should have alternatives like using the data directory itself.
    // or a process running in presto cluster which pushes the data to a database.
    public String scheduleStatsFetchForCompletedQuery(QueryDetail queryDetail)
            throws UnsupportedEncodingException
    {
        log.debug("fetching queryInfo for %s", queryDetail);
        String param = "pretty" + "=" + URLEncoder.encode("true", "UTF-8");
        String target = queryDetail.getClusterUrl() + UI_QUERY_STATS_PATH + queryDetail.getQueryId() + "?" + param;
        String loginURL = queryDetail.getClusterUrl() + LOGIN_PATH;
        String queryInfo = "";
        log.debug("fetching queryInfo for target %s and loginurl %s", target, loginURL);
        Optional<ClusterDetail> cluster = mgr.getAllClusters().stream()
                .filter(c -> c.getClusterUrl().equals(queryDetail.getClusterUrl())).findFirst();
        if (cluster.isEmpty()) {
            return null;
        }
        try {
            int responsecode = PullBasedPrestoClusterMonitor.login(cluster.get());
            log.debug("Logged in");
            // TODO: Set the state in cluster info such that we dont need to login always.
            if (responsecode == HttpStatus.NOT_FOUND.code()) {
                target = queryDetail.getClusterUrl() + UI_QUERY_STATS_PATH_PRESTODB + queryDetail.getQueryId() + "?" + param;
            }
            queryInfo = fetchQueryStats(target);
            connectionManager.open();
            queryDetail.setPrestoQueryInfo(queryInfo);
            synchronized (this) {
                ExecutingQuery.updateQueryInfo(queryDetail);
            }
            log.debug("Successfully stored stats and delete the running query entry.");
        }
        catch (Exception e) {
            log.warn(e, "Caught exception while fetching query stats.");
        }
        finally {
            connectionManager.close();
        }
        return queryInfo;
    }

    private String fetchQueryStats(String target)
            throws IOException
    {
        HttpURLConnection conn = null;
        URL url = new URL(target);
        conn = (HttpURLConnection) url.openConnection();
        if (target.startsWith("https")) {
            setSSLcontext((HttpsURLConnection) conn);
        }
        conn.setConnectTimeout((int) TimeUnit.SECONDS.toMillis(CLUSTER_CONNECT_TIMEOUT_SECONDS));
        conn.setReadTimeout((int) TimeUnit.SECONDS.toMillis(CLUSTER_CONNECT_TIMEOUT_SECONDS));
        conn.setRequestMethod(HttpMethod.GET);
        conn.connect();
        int responseCode = conn.getResponseCode();

        if (responseCode == HttpStatus.OK.code()) {
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader((InputStream) conn.getContent()));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line + "\n");
            }
            String result = sb.toString();

            // TODO: check we can deserialize
            // QueryInfo info = OBJECT_MAPPER.readValue(sb.toString(), QueryInfo.class);

            return result;
        }
        else {
            log.warn("Couldn't connect to the cluster." + target);
            return "";
        }
    }

    @JsonCreator
    public List<QueryDetails2> fetchQueryHistory()
    {
        try {
            connectionManager.open();
            return ExecutingQuery.castToQueryDetails(ExecutingQuery.findAll().limit(2000).orderBy("created desc"));
        }
        finally {
            connectionManager.close();
        }
    }

    @JsonCreator
    public String getClusterForQueryId(String queryId)
    {
        String clusterUrl = null;
        try {
            connectionManager.open();
            ExecutingQuery executingQuery = ExecutingQuery.findById(queryId);
            if (executingQuery != null) {
                clusterUrl = executingQuery.get("cluster_url").toString();
            }
        }
        finally {
            connectionManager.close();
        }
        return clusterUrl;
    }
}
