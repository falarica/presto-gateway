package io.prestosql.gateway.clustermonitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.log.Logger;
import io.prestosql.gateway.ForGateway;
import io.prestosql.gateway.GatewayConfig;
import io.prestosql.gateway.MultiClusterManager;
import io.prestosql.gateway.persistence.ClusterDetail;
import io.prestosql.server.ui.ClusterStatsResource;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.HttpMethod;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

@Singleton
public class PullBasedPrestoClusterMonitor
        implements ClusterMonitor
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final int CLUSTER_CONNECT_TIMEOUT_SECONDS = 15;
    public static final String UI_API_STATS_PATH = "/ui/api/stats";
    public static final String UI_API_STATS_PATH_PRESTODB = "/v1/cluster";
    public static final String LOGIN_PATH = "/ui/login";

    Logger log = Logger.get(PullBasedPrestoClusterMonitor.class);

    private List<ClusterStatsObserver> clusterStatsObservers;
    private MultiClusterManager clusterManager;

    private volatile boolean monitorActive = true;
    private final HttpClient httpClient;

    private ExecutorService executorService = Executors.newCachedThreadPool();
    private ExecutorService singleTaskExecutor = Executors.newSingleThreadExecutor();
    private final ClusterStatsObserver webuiobserver;
    private boolean notLoggedIn = true;
    private final GatewayConfig config;

    @Inject
    public PullBasedPrestoClusterMonitor(@ForGateway HttpClient httpClient, GatewayConfig config,
            MultiClusterManager mgr, ClusterStatsObserver obs)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.clusterManager = requireNonNull(mgr, "clustermgr is null");
        CookieHandler.setDefault(new CookieManager());
        this.clusterStatsObservers = new ArrayList<ClusterStatsObserver>();
        this.webuiobserver = obs;
        this.clusterStatsObservers.add(webuiobserver);
        this.config = requireNonNull(config, "config is null");
        start();
    }

    @PostConstruct
    @Override
    public void monitorClusters()
    {
        start();
    }

    @Override
    public void registerObserver(ClusterStatsObserver ob)
    {
        this.clusterStatsObservers.add(ob);
    }

    @Override
    public void unregisterObserver(ClusterStatsObserver ob)
    {
        this.clusterStatsObservers.remove(ob);
    }

    /**
     * queries all active presto clusters for stats.
     */
    public void start()
    {
        singleTaskExecutor.submit(
                () -> {
                    while (monitorActive) {
                        pullClusterStats();
                        try {
                            Thread.sleep(config.getClusterStatsPullInterval().toMillis());
                        }
                        catch (Exception e) {
                            log.error(e, "Error with monitor task %s");
                        }
                    }
                });
    }

    public void pullClusterStats()
    {
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

            if (clusterStatsObservers != null) {
                for (ClusterStatsObserver observer : clusterStatsObservers) {
                    observer.observe(stats);
                }
            }
        }
        catch (Exception e) {
            log.error(e, "Error performing cluster monitor tasks");
        }
    }

    private SteerDClusterStats getPrestoClusterStats(ClusterDetail cluster)
    {
        SteerDClusterStats steerDClusterStats = new SteerDClusterStats();
        steerDClusterStats.setClusterName(cluster.getName());
        steerDClusterStats.setClusterUrl(cluster.getClusterUrl());
        steerDClusterStats.setLocation(cluster.getLocation());
        String target = cluster.getClusterUrl() + UI_API_STATS_PATH;
        HttpURLConnection conn = null;

        try {
            if (notLoggedIn) {
                login(cluster);
                notLoggedIn = false;
            }
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
                steerDClusterStats.setHealthy(true);
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader((InputStream) conn.getContent()));
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line + "\n");
                }
                String stat = sb.toString();
                ClusterStatsResource.ClusterStats result = OBJECT_MAPPER.readValue(stat,
                        ClusterStatsResource.ClusterStats.class);
                steerDClusterStats.setPrestoClusterStats(result);
                log.debug("Pulled clusterstats : %s", stat);
            }
            else if (responseCode == HttpStatus.UNAUTHORIZED.code()) {
                notLoggedIn = true;
                log.warn("Got unautthorized exception, logging in again");
                login(cluster);
                notLoggedIn = false;
            }
            else if (responseCode == HttpStatus.NOT_FOUND.code()) {
                target = cluster.getClusterUrl() + UI_API_STATS_PATH_PRESTODB;
                URL prestodburl = new URL(target);
                conn = (HttpURLConnection) prestodburl.openConnection();
                if (target.startsWith("https")) {
                    setSSLcontext((HttpsURLConnection) conn);
                }
                conn.setConnectTimeout((int) TimeUnit.SECONDS.toMillis(CLUSTER_CONNECT_TIMEOUT_SECONDS));
                conn.setReadTimeout((int) TimeUnit.SECONDS.toMillis(CLUSTER_CONNECT_TIMEOUT_SECONDS));
                conn.setRequestMethod(HttpMethod.GET);

                conn.connect();
                responseCode = conn.getResponseCode();

                if (responseCode == HttpStatus.OK.code()) {
                    steerDClusterStats.setHealthy(true);
                    BufferedReader reader =
                            new BufferedReader(new InputStreamReader((InputStream) conn.getContent()));
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        sb.append(line + "\n");
                    }
                    String stat = sb.toString();
                    ClusterStatsResource.ClusterStats result = OBJECT_MAPPER.readValue(stat,
                            ClusterStatsResource.ClusterStats.class);
                    steerDClusterStats.setPrestoClusterStats(result);
                    log.debug("Pulled clusterstats : %s", stat);
                }
                else {
                    log.warn("Couldn't find the cluster stats in prestodb as well.");
                }
            }
            else {
                //TODO: if it can't connect then we need to raise error/warning etc.
                log.warn("Received non 200 response, response code: %s", responseCode);
            }
        }
        catch (Exception e) {
            log.warn(e, "Error fetching cluster stats from" + target);
        }
        finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
        return steerDClusterStats;
    }

    public static int login(ClusterDetail cluster)
            throws IOException
    {
        Logger log = Logger.get(PullBasedPrestoClusterMonitor.class);
        String loginURL = cluster.getClusterUrl() + LOGIN_PATH;
        URL obj = new URL(loginURL);

        String password = "";
        if (cluster.getAdminPassword().isPresent()) {
            // TODO: encrypt instead of encode
            Base64.Decoder decoder = Base64.getDecoder();
            byte[] bytes = decoder.decode(cluster.getAdminPassword().get());
            password = new String(bytes, StandardCharsets.UTF_8);
        }
        String postParams = getPrestoFormParams(cluster.getAdminName().orElse("admin"), password);
        HttpURLConnection conn = null;

        conn = (HttpURLConnection) obj.openConnection();
        if (cluster.getClusterUrl().startsWith("https")) {
            setSSLcontext((HttpsURLConnection) conn);
        }
        String useragent = "Mozilla/5.0";

        // Acts like a browser
        conn.setUseCaches(false);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("User-Agent", useragent);
        conn.setRequestProperty("Accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
        conn.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
        conn.setRequestProperty("Connection", "keep-alive");
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setRequestProperty("Content-Length", Integer.toString(postParams.length()));

        conn.setDoOutput(true);
        conn.setDoInput(true);

        // Send post request
        DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
        wr.writeBytes(postParams);
        wr.flush();
        wr.close();

        int responseCode = conn.getResponseCode();
        if (responseCode == HttpStatus.OK.code()) {
            BufferedReader in =
                    new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
        }
        else if (responseCode == HttpStatus.NOT_FOUND.code()) {
            // This is prestodb so ignore
        }
        else {
            log.warn("Couldn't login to " + loginURL);
        }
        return responseCode;
    }

    // Create a trust manager that does not validate certificate chains
    // We trust all the servers that are added by admin.
    public static void setSSLcontext(HttpsURLConnection conn)
    {
        TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager()
                {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers()
                    {
                        return null;
                    }

                    public void checkClientTrusted(
                            java.security.cert.X509Certificate[] certs, String authType)
                    {
                    }

                    public void checkServerTrusted(
                            java.security.cert.X509Certificate[] certs, String authType)
                    {
                    }
                }
        };
        // Install the all-trusting trust manager
        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            conn.setSSLSocketFactory(sc.getSocketFactory());
        }
        catch (Exception e) {
            Logger log = Logger.get(PullBasedPrestoClusterMonitor.class);
            log.warn(e, "Got exception while setting SSL socketFactory");
        }
    }

    public static String getPrestoFormParams(String username, String password)
            throws UnsupportedEncodingException
    {
        List<String> paramList = new ArrayList<String>();
        paramList.add("username" + "=" + URLEncoder.encode(username, "UTF-8"));
        paramList.add("password" + "=" + URLEncoder.encode(password, "UTF-8"));

        StringBuilder result = new StringBuilder();
        for (String param : paramList) {
            if (result.length() == 0) {
                result.append(param);
            }
            else {
                result.append("&" + param);
            }
        }
        return result.toString();
    }

    /**
     * Shut down the app.
     */
    public void stop()
    {
        this.monitorActive = false;
        this.executorService.shutdown();
        this.singleTaskExecutor.shutdown();
    }
}
