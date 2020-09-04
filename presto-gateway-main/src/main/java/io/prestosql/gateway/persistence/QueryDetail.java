package io.prestosql.gateway.persistence;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class QueryDetail
        implements Comparable<QueryDetail>
{
    private String queryId;
    private String queryText;
    private String user;
    private String source;
    private String clusterUrl;
    private long captureTime;
    private String prestoQueryInfo;

    public QueryDetail() {}

    public QueryDetail(@JsonProperty("queryId") String queryId,
            @JsonProperty("queryText") String queryText,
            @JsonProperty("user") String user,
            @JsonProperty("source") String source,
            @JsonProperty("clusterUrl") String clusterUrl,
            @JsonProperty("captureTime") long captureTime,
            @JsonProperty("prestoQueryInfo") String prestoQueryInfo)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        // this.queryText = requireNonNull(queryText, "queryText is null");
        this.queryText = queryText;
        //this.user = requireNonNull(user, "user is null");
        this.user = user;
        //this.source = requireNonNull(source, "source is null");
        this.source = source;
        this.clusterUrl = requireNonNull(clusterUrl, "clusterurl is null");
        this.captureTime = captureTime;
        this.prestoQueryInfo = prestoQueryInfo;
    }

    @Override
    public int compareTo(QueryDetail o)
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
    public String getClusterUrl()
    {
        return clusterUrl;
    }

    @JsonProperty
    public long getCaptureTime()
    {
        return captureTime;
    }

    @JsonProperty
    public String getPrestoQueryInfo()
    {
        return prestoQueryInfo;
    }

    public void setQueryId(String queryId)
    {
        this.queryId = queryId;
    }

    public void setQueryText(String queryText)
    {
        this.queryText = queryText;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setSource(String source)
    {
        this.source = source;
    }

    public void setClusterUrl(String clusterUrl)
    {
        this.clusterUrl = clusterUrl;
    }

    public void setCaptureTime(long captureTime)
    {
        this.captureTime = captureTime;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        QueryDetail that = (QueryDetail) o;
        return Objects.equals(queryId, that.queryId) &&
                Objects.equals(clusterUrl, that.clusterUrl);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(queryId, clusterUrl);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("queryText", queryText)
                .add("clusterUrl", clusterUrl)
                .add("user", user)
                .add("source", source)
                .add("captureTime", captureTime)
                .add("prestoQueryInfo", prestoQueryInfo)
                .omitNullValues()
                .toString();
    }

    public void setPrestoQueryInfo(String queryInfo)
    {
        this.prestoQueryInfo = queryInfo;
    }
}
