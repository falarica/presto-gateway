package io.prestosql.gateway.persistence.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.prestosql.GatewaySessionRepresentation;
import io.prestosql.execution.GatewayQueryStats;
import io.prestosql.execution.QueryState;
import io.prestosql.gateway.persistence.QueryDetail;
import io.prestosql.gateway.persistence.QueryDetails2;
import io.prestosql.server.GatewayBasicQueryInfo;
import io.prestosql.server.GatewayBasicQueryStats;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorType;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.memory.MemoryPoolId;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.IdName;
import org.javalite.activejdbc.annotations.Table;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@IdName("query_id")
@Table("executing_queries")
public class ExecutingQuery
        extends Model
{
    private static final String queryId = "query_id";
    private static final String queryText = "query_text";
    private static final String userName = "user_name";
    private static final String source = "source";
    private static final String clusterUrl = "cluster_url";
    private static final String created = "created";
    private static final String jsonQueryInfo = "query_info";

    public static List<QueryDetails2> castToQueryDetails(List<ExecutingQuery> executingQueryList)
    {
        ObjectMapper mapper = new ObjectMapper();
        Logger log = Logger.get(ExecutingQuery.class);
        JsonCodec<GatewayQueryStats> codec = JsonCodec.jsonCodec(GatewayQueryStats.class);
        JsonCodec<QueryId> queryIdCodec = JsonCodec.jsonCodec(QueryId.class);
        JsonCodec<GatewaySessionRepresentation> sessionCoded = JsonCodec.jsonCodec(GatewaySessionRepresentation.class);

        JsonCodec<ResourceGroupId> resourceGroupIdCodec = JsonCodec.jsonCodec(ResourceGroupId.class);

        JsonCodec<QueryState> queryStateCodec = JsonCodec.jsonCodec(QueryState.class);
        JsonCodec<MemoryPoolId> memPoolCodec = JsonCodec.jsonCodec(MemoryPoolId.class);

        JsonCodec<ErrorType> errTypecodec = JsonCodec.jsonCodec(ErrorType.class);
        JsonCodec<ErrorCode> errCodeCodec = JsonCodec.jsonCodec(ErrorCode.class);

        return executingQueryList.stream().map(q -> {
            JsonNode json = null;
            String queryInfo = q.getString(jsonQueryInfo);
            GatewayBasicQueryInfo bqInfo = null;

            if (queryInfo != null) {
                try {
                    json = mapper.readTree(queryInfo);

                    JsonNode queryStats = json.get("queryStats");
                    JsonNode queryId = json.get("queryId");
                    JsonNode sessionRep = json.get("session");
                    JsonNode resourceGroupId = json.get("resourceGroupId");
                    JsonNode state = json.get("state");
                    JsonNode memPool = json.get("memoryPool");
                    JsonNode scheduled = json.get("scheduled");
                    JsonNode uri = json.get("self");
                    JsonNode query = json.get("query");
                    JsonNode updateType = json.get("updateType");
                    JsonNode preparedQuery = json.get("preparedQuery");
                    JsonNode errorType = json.get("errorType");
                    JsonNode errorCode = json.get("errorCode");
                    GatewayBasicQueryStats stats = new GatewayBasicQueryStats(codec.fromJson(queryStats.toString()));

                    bqInfo = new GatewayBasicQueryInfo(
                            queryIdCodec.fromJson(queryId.toString()),
                            sessionCoded.fromJson(sessionRep.toString()),
                            resourceGroupId == null ? Optional.empty() : Optional.of(resourceGroupIdCodec.fromJson(resourceGroupId.toString())),
                            queryStateCodec.fromJson(state.toString()),
                            memPool == null ? null : memPoolCodec.fromJson(memPool.toString()),
                            scheduled.asBoolean(),
                            mapper.readValue(uri.toString(), URI.class),
                            query.asText(),
                            updateType == null ? Optional.empty() : Optional.of(updateType.asText()),
                            preparedQuery == null ? Optional.empty() : Optional.of(preparedQuery.asText()),
                            stats,
                            errorType == null ? null : errTypecodec.fromJson(errorType.toString()),
                            errorCode == null ? null : errCodeCodec.fromJson(errorCode.toString()));
                }
                catch (JsonProcessingException e) {
                    log.warn(e, "Caught exception while decoding JSON.");
                }
            }
            return new QueryDetails2(
                    q.getString(queryId),
                    q.getString(queryText),
                    q.getString(userName),
                    q.getString(source),
                    q.getString(clusterUrl),
                    q.getLong(created),
                    bqInfo);
        }).collect(Collectors.toList());
    }

    public static void create(QueryDetail queryDetail)
    {
        ExecutingQuery model = new ExecutingQuery();
        model.set(queryId, queryDetail.getQueryId());
        model.set(queryText, queryDetail.getQueryText());
        model.set(clusterUrl, queryDetail.getClusterUrl());
        model.set(userName, queryDetail.getUser());
        model.set(source, queryDetail.getSource());
        model.set(created, queryDetail.getCaptureTime());
        model.set(jsonQueryInfo, queryDetail.getPrestoQueryInfo());
        model.insert();
    }

    public static void updateQueryInfo(QueryDetail queryDetail)
    {
        ExecutingQuery model = new ExecutingQuery();
        model.set(queryId, queryDetail.getQueryId());
        model.set(jsonQueryInfo, queryDetail.getPrestoQueryInfo());
        model.saveIt();
    }
}
