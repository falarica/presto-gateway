/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.gateway;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.slice.Slice;
import io.prestosql.GroupByHashPageIndexerFactory;
import io.prestosql.PagesIndexPageSorter;
import io.prestosql.SystemSessionProperties;
import io.prestosql.client.NodeVersion;
import io.prestosql.connector.ConnectorManager;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.dispatcher.DispatchExecutor;
import io.prestosql.dispatcher.DispatchQueryFactory;
import io.prestosql.dispatcher.FailedDispatchQueryFactory;
import io.prestosql.dispatcher.SteerDDispatchQueryFactory;
import io.prestosql.event.QueryMonitor;
import io.prestosql.event.QueryMonitorConfig;
import io.prestosql.execution.ExecutionFailureInfo;
import io.prestosql.execution.LocationFactory;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.execution.QueryIdGenerator;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.execution.QueryPreparer;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.execution.TaskStatus;
import io.prestosql.execution.resourcegroups.InternalResourceGroupManager;
import io.prestosql.execution.resourcegroups.LegacyResourceGroupConfigurationManager;
import io.prestosql.execution.resourcegroups.ResourceGroupManager;
import io.prestosql.execution.scheduler.NodeScheduler;
import io.prestosql.execution.scheduler.NodeSchedulerConfig;
import io.prestosql.execution.scheduler.TopologyAwareNodeSelectorModule;
import io.prestosql.failuredetector.FailureDetector;
import io.prestosql.failuredetector.NoOpFailureDetector;
import io.prestosql.gateway.clustermonitor.ClusterMonitor;
import io.prestosql.gateway.clustermonitor.ClusterStatsObserver;
import io.prestosql.gateway.clustermonitor.DefaultPrestoClusterStatsObserver;
import io.prestosql.gateway.clustermonitor.PullBasedPrestoClusterMonitor;
import io.prestosql.gateway.persistence.DataStoreConfig;
import io.prestosql.gateway.persistence.JDBCConnectionManager;
import io.prestosql.gateway.querymonitor.PullBasedPrestoQueryExecutionMonitor;
import io.prestosql.gateway.querymonitor.QueryExecutionMonitor;
import io.prestosql.gateway.routing.RoutingManager;
import io.prestosql.gateway.ui.GatewayWebUiModule;
import io.prestosql.index.IndexManager;
import io.prestosql.memory.MemoryManagerConfig;
import io.prestosql.memory.NodeMemoryConfig;
import io.prestosql.memory.SteerDPrestoClusterMemoryManager;
import io.prestosql.metadata.AnalyzePropertyManager;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.ColumnPropertyManager;
import io.prestosql.metadata.HandleJsonModule;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.SchemaPropertyManager;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.metadata.StaticCatalogStore;
import io.prestosql.metadata.StaticCatalogStoreConfig;
import io.prestosql.metadata.TablePropertyManager;
import io.prestosql.operator.GatewayOperatorStats;
import io.prestosql.operator.OperatorStats;
import io.prestosql.operator.PagesIndex;
import io.prestosql.plugin.resourcegroups.db.DbResourceGroupConfig;
import io.prestosql.server.ExpressionSerialization;
import io.prestosql.server.GatewayPluginManager;
import io.prestosql.server.PluginManagerConfig;
import io.prestosql.server.QuerySessionSupplier;
import io.prestosql.server.SessionPropertyDefaults;
import io.prestosql.server.SessionSupplier;
import io.prestosql.server.SliceSerialization;
import io.prestosql.server.remotetask.SteerDHttpLocationFactory;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.memory.ClusterMemoryPoolManager;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.split.PageSinkManager;
import io.prestosql.split.PageSinkProvider;
import io.prestosql.split.PageSourceManager;
import io.prestosql.split.PageSourceProvider;
import io.prestosql.split.SplitManager;
import io.prestosql.sql.SqlEnvironmentConfig;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.parser.SqlParserOptions;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.tree.Expression;
import io.prestosql.transaction.ForTransactionManager;
import io.prestosql.transaction.SteerDTransactionManager;
import io.prestosql.transaction.TransactionManager;
import io.prestosql.transaction.TransactionManagerConfig;
import io.prestosql.type.TypeDeserializer;
import io.prestosql.type.TypeSignatureDeserializer;
import io.prestosql.util.FinalizerService;
import io.prestosql.version.EmbedVersion;

import javax.inject.Singleton;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class GatewayModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfigDefaults(HttpServerConfig.class, httpServerConfig -> {
            httpServerConfig.setAdminEnabled(false);
        });

        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskStatus.class);
        jsonCodecBinder(binder).bindJsonCodec(StageInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(OperatorStats.class);
        jsonCodecBinder(binder).bindJsonCodec(GatewayOperatorStats.class);
        jsonCodecBinder(binder).bindJsonCodec(ExecutionFailureInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(StatsAndCosts.class);

        // handle resolver
        binder.install(new HandleJsonModule());

        httpClientBinder(binder).bindHttpClient("gateway", ForGateway.class);
        // httpClientBinder(binder).bindHttpClient("watcher", ForProxy.class);

        configBinder(binder).bindConfig(GatewayConfig.class);
        configBinder(binder).bindConfig(K8sClusterConfig.class, "k8scluster");
        configBinder(binder).bindConfig(DataStoreConfig.class, "datastore");
        configBinder(binder).bindConfig(JwtHandlerConfig.class, "gateway");
        configBinder(binder).bindConfig(DbResourceGroupConfig.class);

        jaxrsBinder(binder).bind(JDBCConnectionManager.class);
        binder.bind(MultiClusterManager.class).to(PrestoClusterManager.class).in(Scopes.SINGLETON);
        jaxrsBinder(binder).bind(RoutingManager.class);
        jaxrsBinder(binder).bind(QueryHistoryManager.class);

        jaxrsBinder(binder).bind(GatewayResource.class);
        jaxrsBinder(binder).bind(ClusterResource.class);
        jaxrsBinder(binder).bind(ClusterEditorResource.class);
        jaxrsBinder(binder).bind(DefaultPrestoClusterStatsObserver.class);
        jaxrsBinder(binder).bind(SteerDThrowableMapper.class);

        binder.bind(ClusterStatsObserver.class).to(DefaultPrestoClusterStatsObserver.class).in(Scopes.SINGLETON);

        binder.bind(ClusterMonitor.class).to(PullBasedPrestoClusterMonitor.class).in(Scopes.SINGLETON);

        binder.bind(QueryExecutionMonitor.class).to(PullBasedPrestoQueryExecutionMonitor.class).in(Scopes.SINGLETON);

        binder.bind(SteerDPrestoClusterMemoryManager.class).in(Scopes.SINGLETON);
        binder.bind(ClusterMemoryPoolManager.class).to(SteerDPrestoClusterMemoryManager.class).in(Scopes.SINGLETON);

        binder.bind(InternalResourceGroupManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(InternalResourceGroupManager.class).withGeneratedName();
        binder.bind(ResourceGroupManager.class).to(InternalResourceGroupManager.class);
        binder.bind(LegacyResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(PluginManagerConfig.class);
        binder.bind(GatewayPluginManager.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(QueryManagerConfig.class);
        configBinder(binder).bindConfig(TaskManagerConfig.class);
        configBinder(binder).bindConfig(MemoryManagerConfig.class);
        configBinder(binder).bindConfig(FeaturesConfig.class);
        configBinder(binder).bindConfig(NodeMemoryConfig.class);
        configBinder(binder).bindConfig(SqlEnvironmentConfig.class);

        binder.bind(SqlParser.class).in(Scopes.SINGLETON);
        SqlParserOptions sqlParserOptions = new SqlParserOptions();
        //sqlParserOptions.useEnhancedErrorHandler(serverConfig.isEnhancedErrorReporting());
        binder.bind(SqlParserOptions.class).toInstance(sqlParserOptions);

        //binder.bind(StaticCatalogStore.class).in(Scopes.SINGLETON);
        //configBinder(binder).bindConfig(StaticCatalogStoreConfig.class);
        // schema properties
        binder.bind(SchemaPropertyManager.class).in(Scopes.SINGLETON);

        // table properties
        binder.bind(TablePropertyManager.class).in(Scopes.SINGLETON);

        // column properties
        binder.bind(ColumnPropertyManager.class).in(Scopes.SINGLETON);
        // analyze properties
        binder.bind(AnalyzePropertyManager.class).in(Scopes.SINGLETON);

        // Type
        binder.bind(TypeAnalyzer.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonBinder(binder).addDeserializerBinding(TypeSignature.class).to(TypeSignatureDeserializer.class);
        newSetBinder(binder, Type.class);

        binder.bind(MetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(Metadata.class).to(MetadataManager.class).in(Scopes.SINGLETON);

        // slice
        jsonBinder(binder).addSerializerBinding(Slice.class).to(SliceSerialization.SliceSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Slice.class).to(SliceSerialization.SliceDeserializer.class);

        // expression
        jsonBinder(binder).addSerializerBinding(Expression.class).to(ExpressionSerialization.ExpressionSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Expression.class).to(ExpressionSerialization.ExpressionDeserializer.class);

        binder.bind(SessionSupplier.class).to(QuerySessionSupplier.class).in(Scopes.SINGLETON);
        binder.bind(QueryPreparer.class).in(Scopes.SINGLETON);
        binder.bind(QueryIdGenerator.class).in(Scopes.SINGLETON);
        binder.bind(SessionPropertyManager.class).in(Scopes.SINGLETON);
        binder.bind(SystemSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(SessionPropertyDefaults.class).in(Scopes.SINGLETON);
        // execution

        binder.bind(QueryIdGenerator.class).in(Scopes.SINGLETON);
        // binder.bind(QueryManager.class).to(SqlQueryManager.class).in(Scopes.SINGLETON);
        // newExporter(binder).export(QueryManager.class).withGeneratedName();
        binder.bind(QueryPreparer.class).in(Scopes.SINGLETON);
        binder.bind(SessionSupplier.class).to(QuerySessionSupplier.class).in(Scopes.SINGLETON);
        binder.bind(NodeVersion.class).toInstance(new NodeVersion("12"));
        binder.bind(EmbedVersion.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(StatsAndCosts.class);
        configBinder(binder).bindConfig(QueryMonitorConfig.class);
        binder.bind(QueryMonitor.class).in(Scopes.SINGLETON);
        binder.bind(LocationFactory.class).to(SteerDHttpLocationFactory.class).in(Scopes.SINGLETON);

        binder.bind(DispatchQueryFactory.class).to(SteerDDispatchQueryFactory.class);

        // Dispatcher
        //binder.bind(DispatchManager.class).in(Scopes.SINGLETON);
        binder.bind(FailedDispatchQueryFactory.class).in(Scopes.SINGLETON);
        binder.bind(DispatchExecutor.class).in(Scopes.SINGLETON);

        binder.bind(SteerDDispatchManager.class).in(Scopes.SINGLETON);
        // binder.bind(InternalAuthenticationManager.class);
        binder.bind(JsonWebTokenHandler.class).in(Scopes.SINGLETON);
        // binder.bind(ExecutorCleanup.class).in(Scopes.SINGLETON);

        // ******* START - FOR CATALOGS AND DATA CONNECTORS ******* //
        // ***************************************************************** //
        binder.bind(IndexManager.class).in(Scopes.SINGLETON);
        binder.bind(CatalogManager.class).in(Scopes.SINGLETON);
        binder.bind(InternalNodeManager.class).to(InMemoryNodeManager.class).in(Scopes.SINGLETON);
        install(new TopologyAwareNodeSelectorModule());
        configBinder(binder).bindConfig(NodeSchedulerConfig.class);
        binder.bind(FinalizerService.class).in(Scopes.SINGLETON);
        // split manager
        binder.bind(SplitManager.class).in(Scopes.SINGLETON);

        // node partitioning manager
        binder.bind(NodePartitioningManager.class).in(Scopes.SINGLETON);

        // index manager
        binder.bind(IndexManager.class).in(Scopes.SINGLETON);

        binder.bind(FailureDetector.class)
                .to(NoOpFailureDetector.class)
                .in(Scopes.SINGLETON);
        // data stream provider
        binder.bind(PageSourceManager.class).in(Scopes.SINGLETON);
        binder.bind(PageSourceProvider.class).to(PageSourceManager.class).in(Scopes.SINGLETON);

        // page sink provider
        binder.bind(PageSinkManager.class).in(Scopes.SINGLETON);
        binder.bind(PageSinkProvider.class).to(PageSinkManager.class).in(Scopes.SINGLETON);
        binder.bind(NodeScheduler.class).in(Scopes.SINGLETON);
        binder.bind(NodeTaskMap.class).in(Scopes.SINGLETON);
        newExporter(binder).export(NodeScheduler.class).withGeneratedName();

        // PageSorter
        binder.bind(PageSorter.class).to(PagesIndexPageSorter.class).in(Scopes.SINGLETON);

        // PageIndexer
        binder.bind(PageIndexerFactory.class).to(GroupByHashPageIndexerFactory.class).in(Scopes.SINGLETON);
        binder.bind(JoinCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(JoinCompiler.class).withGeneratedName();
        binder.bind(PagesIndex.Factory.class).to(PagesIndex.DefaultFactory.class);
        binder.bind(OrderingCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(OrderingCompiler.class).withGeneratedName();
        binder.bind(StaticCatalogStore.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(StaticCatalogStoreConfig.class);
        binder.bind(ConnectorManager.class).in(Scopes.SINGLETON);
        // ******* END - FOR CATALOGS AND DATA CONNECTORS ******* //
        // ***************************************************************** //

        install(new GatewayWebUiModule());
        configBinder(binder).bindConfig(TransactionManagerConfig.class);
    }

    @Provides
    @Singleton
    @ForTransactionManager
    public static ScheduledExecutorService createTransactionIdleCheckExecutor()
    {
        return newSingleThreadScheduledExecutor(daemonThreadsNamed("transaction-idle-check"));
    }

    @Provides
    @Singleton
    @ForTransactionManager
    public static ExecutorService createTransactionFinishingExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("transaction-finishing-%s"));
    }

    @Provides
    @Singleton
    public static TransactionManager createTransactionManager(
            TransactionManagerConfig config,
            CatalogManager catalogManager,
            MultiClusterManager clusterManager,
            EmbedVersion embedVersion,
            @ForTransactionManager ScheduledExecutorService idleCheckExecutor,
            @ForTransactionManager ExecutorService finishingExecutor)
    {
        return SteerDTransactionManager.create(config, idleCheckExecutor, catalogManager, clusterManager, embedVersion.embedVersion(finishingExecutor));
    }
}
