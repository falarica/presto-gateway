package io.prestosql.gateway.persistence;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.server.GatewayBasicQueryInfo;

import static java.util.Objects.requireNonNull;

public class QueryDetails2
        implements Comparable<QueryDetails2>

{
    private String queryId;
    private String queryText;
    private String user;
    private String source;
    private String clusterUrl;
    private long captureTime;
    private GatewayBasicQueryInfo basicQueryInfo;

    public QueryDetails2(
            @JsonProperty("queryId") String queryId,
            @JsonProperty("queryText") String queryText,
            @JsonProperty("user") String user,
            @JsonProperty("source") String source,
            @JsonProperty("clusterUrl") String clusterUrl,
            @JsonProperty("captureTime") long captureTime,
            @JsonProperty("basicQueryInfo") GatewayBasicQueryInfo basicQueryInfo)

    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.queryText = queryText;
        this.user = user;
        this.source = source;
        this.clusterUrl = requireNonNull(clusterUrl, "clusterurl is null");
        this.captureTime = captureTime;
        this.basicQueryInfo = basicQueryInfo;
    }

    @Override
    public int compareTo(QueryDetails2 o)
    {
        if (this.captureTime < o.captureTime) {
            return 1;
        }
        else {
            return this.captureTime == o.captureTime ? 0 : -1;
        }
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public String getQueryText()
    {
        return queryText;
    }

    @JsonProperty
    public String getUser()
    {
        return user;
    }

    @JsonProperty
    public String getSource()
    {
        return source;
    }

    @JsonProperty
    public long getCaptureTime()
    {
        return captureTime;
    }

    @JsonProperty
    public String getClusterUrl()
    {
        return clusterUrl;
    }

    @JsonProperty
    public GatewayBasicQueryInfo getBasicQueryInfo()
    {
        return basicQueryInfo;
    }
}
