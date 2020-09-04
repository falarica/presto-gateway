package io.prestosql.dispatcher;

import com.google.common.util.concurrent.ListeningExecutorService;
import io.prestosql.Session;
import io.prestosql.execution.QueryPreparer;
import io.prestosql.execution.QueryStateMachine;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.execution.warnings.WarningCollectorFactory;
import io.prestosql.gateway.QueryHistoryManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.server.protocol.Slug;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.transaction.TransactionId;
import io.prestosql.transaction.TransactionManager;

import javax.inject.Inject;

import java.net.URI;
import java.net.URISyntaxException;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.prestosql.util.StatementUtils.isTransactionControlStatement;
import static java.util.Objects.requireNonNull;

public class SteerDDispatchQueryFactory
        implements DispatchQueryFactory
{
    private final ListeningExecutorService executor;

    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Metadata metadata;
    private final WarningCollectorFactory warningCollectorFactory;
    final SqlParser sqlParser;
    final QueryHistoryManager queryHistoryManager;

    @Inject
    public SteerDDispatchQueryFactory(TransactionManager transactionManager,
            AccessControl accessControl,
            Metadata metadata,
            DispatchExecutor dispatchExecutor,
            WarningCollectorFactory warningCollectorFactory,
            SqlParser sqlParser,
            QueryHistoryManager queryHistoryManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.executor = requireNonNull(dispatchExecutor, "executorService is null").getExecutor();
        this.warningCollectorFactory = requireNonNull(warningCollectorFactory, "warningCollectorFactory is null");
        this.sqlParser = sqlParser;
        //this.routingManager = routingManager;
        this.queryHistoryManager = queryHistoryManager;
    }

    @Override
    public DispatchQuery createDispatchQuery(Session session, String query,
            QueryPreparer.PreparedQuery preparedQuery, Slug slug, ResourceGroupId resourceGroup)
    {
        WarningCollector warningCollector = warningCollectorFactory.create();
        URI uri = null;
        try {
            // use dummy uri.
            uri = uriBuilderFrom(new URI("http://localhost:8088"))
                    .appendPath("/v1/query")
                    .appendPath(session.getQueryId().toString())
                    .build();
        }
        catch (URISyntaxException e) {
        }

        QueryStateMachine stateMachine = QueryStateMachine.begin(
                query,
                preparedQuery.getPrepareSql(),
                session,
                uri,
                resourceGroup,
                isTransactionControlStatement(preparedQuery.getStatement()),
                transactionManager,
                accessControl,
                executor,
                metadata,
                warningCollector);

        if (session.getTransactionId().isEmpty()) {
            // TODO: make autocommit isolation level a session parameter
            TransactionId transactionId = transactionManager.beginTransaction(true);
            session = session.beginTransactionId(transactionId, transactionManager, accessControl);
        }
        return new SteerDDispatchedQuery(
                session,
                query,
                preparedQuery,
                slug,
                resourceGroup,
                stateMachine,
                queryHistoryManager);
    }
}
