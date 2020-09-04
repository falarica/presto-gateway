package io.prestosql.gateway.persistence;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.execution.QueryInfo;

import static java.util.Objects.requireNonNull;

public class SteerDQueryInfo
{
    private final String queryId;
    private final String clusterUrl;
    private final QueryInfo prestoQueryInfo;

    public SteerDQueryInfo(
            @JsonProperty("queryId") String queryId,
            @JsonProperty("clusterUrl") String clusterUrl,
            @JsonProperty("prestoQueryInfo") QueryInfo prestoQueryInfo)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.clusterUrl = requireNonNull(clusterUrl, "clusterUrl is null");
        this.prestoQueryInfo = requireNonNull(prestoQueryInfo, "prestoQueryInfo is null");
    }
}
