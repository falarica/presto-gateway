package io.prestosql.transaction;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.prestosql.connector.CatalogName;
import io.prestosql.gateway.MultiClusterManager;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.CatalogMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

public class SteerDTransactionManager
        implements TransactionManager
{
    private static final Logger log = Logger.get(SteerDTransactionManager.class);

    private final TransactionManager mgr;

    private final MultiClusterManager clusterManager;

    private SteerDTransactionManager(TransactionManagerConfig config,
            ScheduledExecutorService idleCheckExecutor,
            CatalogManager catalogManager,
            MultiClusterManager clusterManager,
            Executor finishingExecutor)
    {
        this.mgr = InMemoryTransactionManager.create(config, idleCheckExecutor, catalogManager, finishingExecutor);
        this.clusterManager = clusterManager;
        log.debug("The idle timeout is " + config.getIdleTimeout() + " the idle check interval is " + config.getIdleCheckInterval());
        // TODO: schedule expire checks so that we can clean inactive tx.
        //transactionManager.scheduleIdleChecks(config.getIdleCheckInterval(), idleCheckExecutor);
    }

    public static TransactionManager create(
            TransactionManagerConfig config,
            ScheduledExecutorService idleCheckExecutor,
            CatalogManager catalogManager,
            MultiClusterManager clusterManager,
            Executor finishingExecutor)
    {
        SteerDTransactionManager transactionManager = new SteerDTransactionManager(config, idleCheckExecutor, catalogManager, clusterManager, finishingExecutor);
        return transactionManager;
    }

    @Override
    public boolean transactionExists(TransactionId transactionId)
    {
        return this.mgr.transactionExists(transactionId);
    }

    @Override
    public TransactionInfo getTransactionInfo(TransactionId transactionId)
    {
        return this.mgr.getTransactionInfo(transactionId);
    }

    @Override
    public List<TransactionInfo> getAllTransactionInfos()
    {
        return this.mgr.getAllTransactionInfos();
    }

    @Override
    public TransactionId beginTransaction(boolean autoCommitContext)
    {
        return this.mgr.beginTransaction(autoCommitContext);
    }

    @Override
    public TransactionId beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommitContext)
    {
        return this.mgr.beginTransaction(isolationLevel, readOnly, autoCommitContext);
    }

    @Override
    public Map<String, CatalogName> getCatalogNames(TransactionId transactionId)
    {
        return this.mgr.getCatalogNames(transactionId);
    }

    @Override
    public Optional<CatalogMetadata> getOptionalCatalogMetadata(TransactionId transactionId, String catalogName)
    {
        return this.mgr.getOptionalCatalogMetadata(transactionId, catalogName);
    }

    @Override
    public CatalogMetadata getCatalogMetadata(TransactionId transactionId, CatalogName catalogName)
    {
        return this.mgr.getCatalogMetadata(transactionId, catalogName);
    }

    @Override
    public CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, CatalogName catalogName)
    {
        return this.mgr.getCatalogMetadataForWrite(transactionId, catalogName);
    }

    @Override
    public CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, String catalogName)
    {
        return this.mgr.getCatalogMetadataForWrite(transactionId, catalogName);
    }

    @Override
    public ConnectorTransactionHandle getConnectorTransaction(TransactionId transactionId, CatalogName catalogName)
    {
        return this.mgr.getConnectorTransaction(transactionId, catalogName);
    }

    @Override
    public void checkAndSetActive(TransactionId transactionId)
    {
    }

    @Override
    public void trySetActive(TransactionId transactionId)
    {
    }

    @Override
    public void trySetInactive(TransactionId transactionId)
    {
    }

    @Override
    public ListenableFuture<?> asyncCommit(TransactionId transactionId)
    {
        return this.mgr.asyncCommit(transactionId);
    }

    @Override
    public ListenableFuture<?> asyncAbort(TransactionId transactionId)
    {
        return this.mgr.asyncAbort(transactionId);
    }

    @Override
    public void fail(TransactionId transactionId)
    {
        this.mgr.fail(transactionId);
    }
}
