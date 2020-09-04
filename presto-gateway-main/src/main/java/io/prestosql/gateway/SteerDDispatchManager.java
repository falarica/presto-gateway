package io.prestosql.gateway;

import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.dispatcher.DispatchExecutor;
import io.prestosql.dispatcher.DispatchInfo;
import io.prestosql.dispatcher.DispatchQuery;
import io.prestosql.dispatcher.DispatchQueryFactory;
import io.prestosql.dispatcher.FailedDispatchQueryFactory;
import io.prestosql.dispatcher.SteerDDispatchedQuery;
import io.prestosql.execution.GatewayQueryTracker;
import io.prestosql.execution.QueryIdGenerator;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.execution.QueryManagerStats;
import io.prestosql.execution.QueryPreparer;
import io.prestosql.execution.resourcegroups.ResourceGroupManager;
import io.prestosql.gateway.persistence.QueryDetails2;
import io.prestosql.gateway.routing.RoutingManager;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.security.AccessControl;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.GatewayRequestSessionContext;
import io.prestosql.server.SessionContext;
import io.prestosql.server.SessionPropertyDefaults;
import io.prestosql.server.SessionSupplier;
import io.prestosql.server.protocol.Slug;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.resourcegroups.QueryType;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.resourcegroups.SelectionContext;
import io.prestosql.spi.resourcegroups.SelectionCriteria;
import io.prestosql.sql.tree.CreateRole;
import io.prestosql.sql.tree.CreateView;
import io.prestosql.sql.tree.DropRole;
import io.prestosql.sql.tree.DropView;
import io.prestosql.transaction.TransactionManager;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.prestosql.spi.StandardErrorCode.QUERY_TEXT_TOO_LARGE;
import static io.prestosql.spi.security.AccessDeniedException.denyCatalogAccess;
import static io.prestosql.util.StatementUtils.getQueryType;
import static io.prestosql.util.StatementUtils.isTransactionControlStatement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SteerDDispatchManager
{
    private static final Logger log = Logger.get(SteerDDispatchManager.class);

    private final QueryIdGenerator queryIdGenerator;
    private final QueryPreparer queryPreparer;
    private final ResourceGroupManager resourceGroupManager;
    private final DispatchQueryFactory dispatchQueryFactory;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final SessionSupplier sessionSupplier;
    private final SessionPropertyDefaults sessionPropertyDefaults;
    private final int maxQueryLength;
    private final FailedDispatchQueryFactory failedDispatchQueryFactory;

    private final Executor queryExecutor;

    private final GatewayQueryTracker<DispatchQuery> queryTracker;

    private final QueryManagerStats stats = new QueryManagerStats();

    private final RoutingManager routingManager;

    private final MultiClusterManager prestoClusterMgr;
    private JsonCodec<QueryInfo> queryInfoCodec;

    @Inject
    public SteerDDispatchManager(
            QueryIdGenerator queryIdGenerator,
            QueryPreparer queryPreparer,
            ResourceGroupManager resourceGroupManager,
            DispatchQueryFactory dispatchQueryFactory,
            TransactionManager transactionManager,
            AccessControl accessControl,
            SessionSupplier sessionSupplier,
            SessionPropertyDefaults sessionPropertyDefaults,
            QueryManagerConfig queryManagerConfig,
            DispatchExecutor dispatchExecutor,
            RoutingManager routingManager,
            MultiClusterManager prestoClusterMgr,
            FailedDispatchQueryFactory failedDispatchQueryFactory)
    {
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.queryPreparer = requireNonNull(queryPreparer, "queryPreparer is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.dispatchQueryFactory = requireNonNull(dispatchQueryFactory, "dispatchQueryFactory is null");
        //this.failedDispatchQueryFactory = failedDispatchQueryFactory;//requireNonNull(failedDispatchQueryFactory, "failedDispatchQueryFactory is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
        this.sessionPropertyDefaults = requireNonNull(sessionPropertyDefaults, "sessionPropertyDefaults is null");

        requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.maxQueryLength = queryManagerConfig.getMaxQueryLength();

        this.queryExecutor = requireNonNull(dispatchExecutor, "dispatchExecutor is null").getExecutor();

        this.queryTracker = new GatewayQueryTracker<>(queryManagerConfig, dispatchExecutor.getScheduledExecutor());
        this.routingManager = routingManager;
        this.prestoClusterMgr = prestoClusterMgr;
        this.failedDispatchQueryFactory = failedDispatchQueryFactory;
    }

    @PostConstruct
    public void start()
    {
        queryTracker.start();
    }

    private boolean queryCreated(DispatchQuery dispatchQuery)
    {
        boolean queryAdded = queryTracker.addQuery(dispatchQuery);

        // only add state tracking if this query instance will actually be used for the execution
        if (queryAdded) {
            dispatchQuery.addStateChangeListener(newState -> {
                if (newState.isDone()) {
                    // execution MUST be added to the expiration queue or there will be a leak
                    queryTracker.expireQuery(dispatchQuery.getQueryId());
                }
            });
            stats.trackQueryStats(dispatchQuery);
        }

        return queryAdded;
    }

    public void dispatchQuery(QueryId queryId, Slug slug, SessionContext sessionContext, String query)
            throws URISyntaxException
    {
        Session session = null;
        QueryPreparer.PreparedQuery preparedQuery = null;
        try {
            if (query.length() > maxQueryLength) {
                int queryLength = query.length();
                query = query.substring(0, maxQueryLength);
                throw new PrestoException(QUERY_TEXT_TOO_LARGE, format("Query text length (%s) exceeds the maximum length (%s)", queryLength, maxQueryLength));
            }

            // decode session
            session = sessionSupplier.createSession(queryId, sessionContext);

            // check query execute permissions
            accessControl.checkCanExecuteQuery(sessionContext.getIdentity());
            Set<String> catalogs = ((GatewayRequestSessionContext) sessionContext).getQueryCatalogs();

            Set<String> extraCatalogs = accessControl.filterCatalogs(sessionContext.getIdentity(), catalogs);
            if (extraCatalogs.size() != catalogs.size()) {
                catalogs.removeAll(extraCatalogs);
                denyCatalogAccess(catalogs.toString());
            }
            // prepare query
            preparedQuery = queryPreparer.prepareQuery(session, query);

            // select resource group
            Set<Class> ddlWithStates = new HashSet<>();
            ddlWithStates.add(CreateRole.class);
            ddlWithStates.add(DropRole.class);
            ddlWithStates.add(CreateView.class);
            ddlWithStates.add(DropView.class);

            Optional<String> queryType = Optional.empty();
            if (ddlWithStates.contains(preparedQuery.getStatement().getClass())) {
                queryType = getQueryType(preparedQuery.getStatement().getClass()).map(Enum::name);
                log.debug("The queryType is %s", queryType.isPresent() ? queryType.get() : "NO QueryType");
            }

            SelectionContext selectionContext = resourceGroupManager.selectGroup(new SelectionCriteria(
                    sessionContext.getIdentity().getPrincipal().isPresent(),
                    sessionContext.getIdentity().getUser(),
                    sessionContext.getIdentity().getGroups(),
                    Optional.ofNullable(sessionContext.getSource()),
                    sessionContext.getClientTags(),
                    sessionContext.getResourceEstimates(),
                    queryType));

            log.debug("The resource group is " + selectionContext.getResourceGroupId());
            // apply system default session properties (does not override user set properties)
            session = sessionPropertyDefaults.newSessionWithDefaultProperties(session, queryType, selectionContext.getResourceGroupId());
            // mark existing transaction as active
            transactionManager.activateTransaction(session, isTransactionControlStatement(preparedQuery.getStatement()), accessControl);

            DispatchQuery dispatchQuery = dispatchQueryFactory.createDispatchQuery(
                    session,
                    query,
                    preparedQuery,
                    slug,
                    selectionContext.getResourceGroupId());

            // Apply all the Routing rules here.
            // The query should be submitted only if it can get at least one cluster
            // to be executed on.
            // Once it gets at least one cluster for execution, it will submit and
            // check for resources.

            URI routedURI = routingManager.providePrestoCluster(
                    (GatewayRequestSessionContext) sessionContext, Optional.of(session));
            // routedURI can change in some cases, specially if the query is queued
            // after queuing, maybe we need to again check for the cluster
            // and update the URI. as cloud burst or resource availability in some clusters
            // might change the routing decision taken now.

            ((SteerDDispatchedQuery) dispatchQuery).setRequiredCatalogs(catalogs);
            if (queryType.isPresent() && queryType.get().equals(QueryType.DATA_DEFINITION)) {
                ((SteerDDispatchedQuery) dispatchQuery).setRoutedURI(routedURI);
            }
            else {
                ((SteerDDispatchedQuery) dispatchQuery).setRoutedURI(routedURI);
            }
            boolean queryAdded = queryCreated(dispatchQuery);

            try {
                resourceGroupManager.submit(dispatchQuery, selectionContext, /*queryExecutor*/directExecutor());
            }
            catch (Throwable e) {
                // dispatch query has already been registered, so just fail it directly
                dispatchQuery.fail(e);
            }
        }
        catch (Throwable throwable) {
            if (session == null) {
                session = Session.builder(new SessionPropertyManager())
                        .setQueryId(queryId)
                        .setIdentity(sessionContext.getIdentity())
                        .setSource(sessionContext.getSource())
                        .build();
            }
            Optional<String> preparedSql = Optional.ofNullable(preparedQuery).flatMap(QueryPreparer.PreparedQuery::getPrepareSql);
            DispatchQuery failedDispatchQuery = failedDispatchQueryFactory.createFailedDispatchQuery(session, query, preparedSql, Optional.empty(), throwable);
            queryCreated(failedDispatchQuery);
        }
    }

    public QueryId createQueryId()
    {
        return queryIdGenerator.createNextQueryId();
    }

    public boolean isQueryRegistered(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId).isPresent();
    }

    public DispatchQuery getQuery(QueryId queryId)
    {
        return queryTracker.getQuery(queryId);
    }

    public BasicQueryInfo getQueryInfo(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getBasicQueryInfo();
    }

    public Optional<QueryInfo> getFullQueryInfo(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId).map(DispatchQuery::getFullQueryInfo);
    }

    public Optional<DispatchInfo> getDispatchInfo(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(dispatchQuery -> {
                    dispatchQuery.recordHeartbeat();
                    return dispatchQuery.getDispatchInfo();
                });
    }

    public void cancelQuery(QueryId queryId)
    {
        queryTracker.tryGetQuery(queryId)
                .ifPresent(DispatchQuery::cancel);
        queryTracker.expireQuery(queryId);
    }

    public void failQuery(QueryId queryId, Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        queryTracker.tryGetQuery(queryId)
                .ifPresent(query -> query.fail(cause));
        queryTracker.expireQuery(queryId);
    }

    public void querySubmitted(QueryId queryId)
    {
        queryTracker.tryGetQuery(queryId).ifPresent(query -> ((SteerDDispatchedQuery) query).queryStarted());
    }

    public void queryFinished(QueryId queryId, String qInfo)
    {
        log.debug("Marking the query to be finished %s", queryId.toString());
        queryTracker.tryGetQuery(queryId).ifPresent(query -> ((SteerDDispatchedQuery) query).queryFinished(qInfo));
        queryTracker.expireQuery(queryId);
    }

    public List<QueryDetails2> getQueries()
    {
        return prestoClusterMgr.getAllQueries();
    }

    public void setQueryInfoCodec(JsonCodec<QueryInfo> codec)
    {
        this.queryInfoCodec = codec;
    }

    public void storeStartedTransactionIdAgainstTheCluster()
    {
    }

    public Optional<ResourceGroupId> getResourceGroupForQuery(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(query -> ((SteerDDispatchedQuery) query).getRsId());
    }

    public void updateHeartbeat(QueryId queryId)
    {
        queryTracker.tryGetQuery(queryId)
                .ifPresent(query -> ((SteerDDispatchedQuery) query).updateHeartBeat());
    }
}
