package io.prestosql.dispatcher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.ExecutionFailureInfo;
import io.prestosql.execution.GatewayQueryStats;
import io.prestosql.execution.QueryExecution;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryPreparer;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.QueryStateMachine;
import io.prestosql.execution.StateMachine;
import io.prestosql.gateway.QueryHistoryManager;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.protocol.Slug;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.prestosql.execution.QueryState.FAILED;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.util.Failures.toFailure;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SteerDDispatchedQuery
        implements DispatchQuery
{
    private static final Logger log = Logger.get(SteerDDispatchedQuery.class);
    private final DateTime createTime = DateTime.now();
    private DateTime currentHeartbeat = DateTime.now();
    private Optional<DateTime> endTime;
    private boolean isDone;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final QueryStateMachine stateMachine;

    private final StateMachine<QueryState> queryState;

    private final ListenableFuture<QueryExecution> queryExecutionFuture;
    private final Session session;
    final QueryHistoryManager queryHistoryManager;

    private final Set<String> requiredCatalogs;
    private URI routedURI;

    private boolean dispatched;
    private String queryInfo;

    private final ResourceGroupId rsId;

    public SteerDDispatchedQuery(Session session,
            String query, QueryPreparer.PreparedQuery preparedQuery,
            Slug slug, ResourceGroupId resourceGroup, QueryStateMachine stateMachine,
            QueryHistoryManager queryHistoryManager)
    {
        this.session = session;
        this.queryExecutionFuture = null;
        this.queryState = null;
        this.requiredCatalogs = new HashSet<>();
        this.stateMachine = stateMachine;
        this.queryHistoryManager = queryHistoryManager;
        this.rsId = resourceGroup;
        this.endTime = Optional.empty();
    }

    @Override
    public void recordHeartbeat()
    {
    }

    @Override
    public ListenableFuture<?> getDispatchedFuture()
    {
        return null;
    }

    public ResourceGroupId getRsId()
    {
        return rsId;
    }

    @Override
    public DispatchInfo getDispatchInfo()
    {
        // observe submitted before getting the state, to ensure a failed query stat is visible
        BasicQueryInfo queryInfo = stateMachine.getBasicQueryInfo(Optional.empty());
        // Just check the state

        if (queryInfo.getState() == FAILED) {
            ExecutionFailureInfo failureInfo = stateMachine.getFailureInfo()
                    .orElseGet(() -> toFailure(new PrestoException(GENERIC_INTERNAL_ERROR, "Query failed for an unknown reason")));

            return DispatchInfo.failed(failureInfo, queryInfo.getQueryStats().getElapsedTime(), queryInfo.getQueryStats().getQueuedTime());
        }
        if (dispatched) {
            // Get the co-ordinator location from routing manager.
            return DispatchInfo.dispatched(new RoutedCoordinatorLocation(this.routedURI),
                    new Duration(0, MILLISECONDS),
                    new Duration(0, MILLISECONDS));
        }
        return DispatchInfo.queued(queryInfo.getQueryStats().getElapsedTime(), queryInfo.getQueryStats().getQueuedTime());
    }

    @Override
    public void cancel()
    {
    }

    @Override
    public void startWaitingForResources()
    {
        // use query to get all the catalogs.
        // wait for atleast cluster where it can be executed.
        // use routing manager to get the cluster.
        // if no cluster is ready then either throw exception
        // or wait with status waiting for resources.

        // once again check for the routing rules and see which URI to
        // route the query to.
        this.dispatched = true;
        // ideally it should be dispatched from here. or wait till it gets dispatched.
        // Check from the database if it is dispatched. We need to map the queryId
        // generated here with the queryId in the routed cluster.
        // in the next state we need to
        stateMachine.transitionToRunning();
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener)
    {
        log.debug("The state change listener is called..");
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return DataSize.ofBytes(0);
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return DataSize.ofBytes(0);
    }

    @Override
    public Duration getTotalCpuTime()
    {
        if (this.queryInfo != null && !this.queryInfo.isEmpty()) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonCodec<GatewayQueryStats> codec = JsonCodec.jsonCodec(GatewayQueryStats.class);
                JsonNode json = mapper.readTree(this.queryInfo);
                JsonNode queryStats = json.get("queryStats");
                GatewayQueryStats stats2 = codec.fromJson(queryStats.toString());
                log.debug("The cputime of the query is %s", stats2.getTotalCpuTime());

                return stats2.getTotalCpuTime();
            }
            catch (JsonProcessingException e) {
                log.warn(e, "Got exception while decoding JSON.");
            }
            return new Duration(10, SECONDS);
        }
        else {
            return new Duration(0, MILLISECONDS);
        }
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        // All this will be populated from the DB or remote call
        return tryGetQueryExecution()
                .map(QueryExecution::getBasicQueryInfo)
                .orElseGet(() -> stateMachine.getBasicQueryInfo(Optional.empty()));
    }

    @Override
    public QueryInfo getFullQueryInfo()
    {
        // All this will be populated from the DB or remote call
        return tryGetQueryExecution()
                .map(QueryExecution::getQueryInfo)
                .orElseGet(() -> stateMachine.updateQueryInfo(Optional.empty()));
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return Optional.empty();
    }

    @Override
    public QueryId getQueryId()
    {
        return session.getQueryId();
    }

    @Override
    public boolean isDone()
    {
        // If it has been done..we need to check from database that this query has been
        // submitted and finished.
        return isDone;
    }

    @Override
    public Session getSession()
    {
        return session;
    }

    @Override
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return Optional.of(createTime);
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        // provide last heartbeat depending on the return from the remote presto cluster.
        return this.currentHeartbeat;
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return this.endTime;
    }

    @Override
    public void fail(Throwable cause)
    {
        stateMachine.transitionToFailed(cause);
        this.endTime = Optional.of(DateTime.now());
        this.isDone = true;
    }

    @Override
    public void pruneInfo()
    {
    }

    private Optional<QueryExecution> tryGetQueryExecution()
    {
        try {
            return tryGetFutureValue(queryExecutionFuture);
        }
        catch (Exception ignored) {
            return Optional.empty();
        }
    }

    public void setRequiredCatalogs(Set<String> catalogs)
    {
        this.requiredCatalogs.addAll(catalogs);
    }

    public void setRoutedURI(URI routedURI)
    {
        this.routedURI = routedURI;
    }

    public URI getRoutedURI()
    {
        return routedURI;
    }

    public void queryStarted()
    {
        stateMachine.transitionToRunning();
    }

    public void queryFinished(String qInfo)
    {
        this.queryInfo = qInfo;
        stateMachine.transitionToFinishing();
        this.endTime = Optional.of(DateTime.now());
        isDone = true;
    }

    public void updateHeartBeat()
    {
        this.currentHeartbeat = DateTime.now();
    }
}
