package io.prestosql.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.base.security.AllowAllSystemAccessControl;
import io.prestosql.plugin.base.security.FileBasedSystemAccessControl;
import io.prestosql.plugin.base.security.ForwardingSystemAccessControl;
import io.prestosql.plugin.base.security.ReadOnlySystemAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;
import io.prestosql.spi.security.SystemSecurityContext;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.transaction.TransactionManager;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.prestosql.spi.StandardErrorCode.SERVER_STARTING_UP;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SteerDAccessControlManager
        extends AccessControlManager
{
    private static final Logger log = Logger.get(AccessControlManager.class);

    private static final File CONFIG_FILE = new File("etc/access-control.properties");
    private static final String NAME_PROPERTY = "access-control.name";

    private final List<File> configFiles;
    private final Map<String, SystemAccessControlFactory> systemAccessControlFactories = new ConcurrentHashMap<>();

    private final AtomicReference<List<SystemAccessControl>> systemAccessControls = new AtomicReference<>(ImmutableList.of(new SteerDAccessControlManager.InitializingSystemAccessControl()));
    private final AtomicBoolean systemAccessControlLoading = new AtomicBoolean();

    private final CounterStat authorizationSuccess = new CounterStat();
    private final CounterStat authorizationFail = new CounterStat();

    @Inject
    public SteerDAccessControlManager(TransactionManager transactionManager, AccessControlConfig config)
    {
        super(transactionManager, config);
        this.configFiles = ImmutableList.copyOf(config.getAccessControlFiles());
        addSteerDSystemAccessControlFactory(new AllowAllSystemAccessControl.Factory());
        addSteerDSystemAccessControlFactory(new ReadOnlySystemAccessControl.Factory());
        addSteerDSystemAccessControlFactory(new FileBasedSystemAccessControl.Factory());
    }

    public final void addSteerDSystemAccessControlFactory(SystemAccessControlFactory accessControlFactory)
    {
        requireNonNull(accessControlFactory, "accessControlFactory is null");

        if (systemAccessControlFactories.putIfAbsent(accessControlFactory.getName(), accessControlFactory) != null) {
            throw new IllegalArgumentException(format("Access control '%s' is already registered", accessControlFactory.getName()));
        }
    }

    public void loadSystemAccessControl()
    {
        List<File> configFiles = this.configFiles;
        if (configFiles.isEmpty()) {
            if (!CONFIG_FILE.exists()) {
                setSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());
                log.info("Using system access control %s", AllowAllSystemAccessControl.NAME);
                return;
            }
            configFiles = ImmutableList.of(CONFIG_FILE);
        }
        checkState(systemAccessControlLoading.compareAndSet(false, true), "System access control already initialized");

        List<SystemAccessControl> systemAccessControls = configFiles.stream()
                .map(this::createSystemAccessControl)
                .collect(toImmutableList());

        this.systemAccessControls.set(systemAccessControls);
    }

    private SystemAccessControl createSystemAccessControl(File configFile)
    {
        log.info("-- Loading system access control %s --", configFile);
        configFile = configFile.getAbsoluteFile();

        Map<String, String> properties;
        try {
            properties = new HashMap<>(loadPropertiesFrom(configFile.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read configuration file: " + configFile, e);
        }

        String name = properties.remove(NAME_PROPERTY);
        log.info("Loading the name: %s ", name);
        checkState(!isNullOrEmpty(name), "Access control configuration does not contain '%s' property: %s", NAME_PROPERTY, configFile);

        SystemAccessControlFactory systemAccessControlFactory = systemAccessControlFactories.get(name);
        checkState(systemAccessControlFactory != null, "Access control '%s' is not registered: %s", name, configFile);

        SystemAccessControl systemAccessControl = systemAccessControlFactory.create(ImmutableMap.copyOf(properties));
        log.info("-- Loaded system access control %s --", name);
        return systemAccessControl;
    }

    @VisibleForTesting
    protected void setSystemAccessControl(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        checkState(systemAccessControlLoading.compareAndSet(false, true), "System access control already initialized");

        SystemAccessControlFactory systemAccessControlFactory = systemAccessControlFactories.get(name);
        checkState(systemAccessControlFactory != null, "Access control '%s' is not registered", name);

        SystemAccessControl systemAccessControl = systemAccessControlFactory.create(ImmutableMap.copyOf(properties));
        this.systemAccessControls.set(ImmutableList.of(systemAccessControl));
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(userName, "userName is null");

        systemAuthorizationCheck(control -> control.checkCanImpersonateUser(new SystemSecurityContext(identity, Optional.empty()), userName));
    }

    @Override
    @Deprecated
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        requireNonNull(principal, "principal is null");
        requireNonNull(userName, "userName is null");

        systemAuthorizationCheck(control -> control.checkCanSetUser(principal, userName));
    }

    @Override
    public void checkCanExecuteQuery(Identity identity)
    {
        requireNonNull(identity, "identity is null");

        systemAuthorizationCheck(control -> control.checkCanExecuteQuery(new SystemSecurityContext(identity, Optional.empty())));
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, String queryOwner)
    {
        requireNonNull(identity, "identity is null");

        systemAuthorizationCheck(control -> control.checkCanViewQueryOwnedBy(new SystemSecurityContext(identity, Optional.empty()), queryOwner));
    }

    @Override
    public Set<String> filterQueriesOwnedBy(Identity identity, Set<String> queryOwners)
    {
        for (SystemAccessControl systemAccessControl : systemAccessControls.get()) {
            queryOwners = systemAccessControl.filterViewQueryOwnedBy(new SystemSecurityContext(identity, Optional.empty()), queryOwners);
        }
        return queryOwners;
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, String queryOwner)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(queryOwner, "queryOwner is null");

        systemAuthorizationCheck(control -> control.checkCanKillQueryOwnedBy(new SystemSecurityContext(identity, Optional.empty()), queryOwner));
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogs, "catalogs is null");

        for (SystemAccessControl systemAccessControl : systemAccessControls.get()) {
            catalogs = systemAccessControl.filterCatalogs(new SystemSecurityContext(identity, Optional.empty()), catalogs);
        }
        return catalogs;
    }

    @Override
    public void checkCanCreateSchema(SecurityContext securityContext, CatalogSchemaName schemaName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schemaName, "schemaName is null");

        checkCanAccessCatalog(securityContext, schemaName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanCreateSchema(securityContext.toSystemSecurityContext(), schemaName));
    }

    @Override
    public void checkCanDropSchema(SecurityContext securityContext, CatalogSchemaName schemaName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schemaName, "schemaName is null");

        checkCanAccessCatalog(securityContext, schemaName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanDropSchema(securityContext.toSystemSecurityContext(), schemaName));
    }

    @Override
    public void checkCanRenameSchema(SecurityContext securityContext, CatalogSchemaName schemaName, String newSchemaName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schemaName, "schemaName is null");

        checkCanAccessCatalog(securityContext, schemaName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanRenameSchema(securityContext.toSystemSecurityContext(), schemaName, newSchemaName));
    }

    @Override
    public void checkCanSetSchemaAuthorization(SecurityContext securityContext, CatalogSchemaName schemaName, PrestoPrincipal principal)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schemaName, "schemaName is null");

        checkCanAccessCatalog(securityContext, schemaName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanSetSchemaAuthorization(securityContext.toSystemSecurityContext(), schemaName, principal));
    }

    @Override
    public void checkCanShowSchemas(SecurityContext securityContext, String catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");

        checkCanAccessCatalog(securityContext, catalogName);

        systemAuthorizationCheck(control -> control.checkCanShowSchemas(securityContext.toSystemSecurityContext(), catalogName));
    }

    @Override
    public Set<String> filterSchemas(SecurityContext securityContext, String catalogName, Set<String> schemaNames)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaNames, "schemaNames is null");

        if (filterCatalogs(securityContext.getIdentity(), ImmutableSet.of(catalogName)).isEmpty()) {
            return ImmutableSet.of();
        }

        for (SystemAccessControl systemAccessControl : systemAccessControls.get()) {
            schemaNames = systemAccessControl.filterSchemas(securityContext.toSystemSecurityContext(), catalogName, schemaNames);
        }
        return schemaNames;
    }

    @Override
    public void checkCanShowCreateSchema(SecurityContext securityContext, CatalogSchemaName schemaName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schemaName, "schemaName is null");

        checkCanAccessCatalog(securityContext, schemaName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanShowCreateSchema(securityContext.toSystemSecurityContext(), schemaName));
    }

    @Override
    public void checkCanShowCreateTable(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanShowCreateTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));
    }

    @Override
    public void checkCanCreateTable(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanCreateTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));
    }

    @Override
    public void checkCanDropTable(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanDropTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));
    }

    @Override
    public void checkCanRenameTable(SecurityContext securityContext, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newTableName, "newTableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanRenameTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), newTableName.asCatalogSchemaTableName()));
    }

    @Override
    public void checkCanSetTableComment(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanSetTableComment(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));
    }

    @Override
    public void checkCanShowTables(SecurityContext securityContext, CatalogSchemaName schema)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schema, "schema is null");

        checkCanAccessCatalog(securityContext, schema.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanShowTables(securityContext.toSystemSecurityContext(), schema));
    }

    @Override
    public Set<SchemaTableName> filterTables(SecurityContext securityContext, String catalogName, Set<SchemaTableName> tableNames)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(tableNames, "tableNames is null");

        if (filterCatalogs(securityContext.getIdentity(), ImmutableSet.of(catalogName)).isEmpty()) {
            return ImmutableSet.of();
        }

        for (SystemAccessControl systemAccessControl : systemAccessControls.get()) {
            tableNames = systemAccessControl.filterTables(securityContext.toSystemSecurityContext(), catalogName, tableNames);
        }

        return tableNames;
    }

    @Override
    public void checkCanShowColumns(SecurityContext securityContext, CatalogSchemaTableName table)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(table, "table is null");

        checkCanAccessCatalog(securityContext, table.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanShowColumns(securityContext.toSystemSecurityContext(), table));
    }

    @Override
    public List<ColumnMetadata> filterColumns(SecurityContext securityContext, CatalogSchemaTableName table, List<ColumnMetadata> columns)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(table, "tableName is null");

        if (filterTables(securityContext, table.getCatalogName(), ImmutableSet.of(table.getSchemaTableName())).isEmpty()) {
            return ImmutableList.of();
        }

        for (SystemAccessControl systemAccessControl : systemAccessControls.get()) {
            columns = systemAccessControl.filterColumns(securityContext.toSystemSecurityContext(), table, columns);
        }

        return columns;
    }

    @Override
    public void checkCanAddColumns(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanAddColumn(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));
    }

    @Override
    public void checkCanDropColumn(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanDropColumn(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));
    }

    @Override
    public void checkCanRenameColumn(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanRenameColumn(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));
    }

    @Override
    public void checkCanInsertIntoTable(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanInsertIntoTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));
    }

    @Override
    public void checkCanDeleteFromTable(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanDeleteFromTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));
    }

    @Override
    public void checkCanCreateView(SecurityContext securityContext, QualifiedObjectName viewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(viewName, "viewName is null");

        checkCanAccessCatalog(securityContext, viewName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanCreateView(securityContext.toSystemSecurityContext(), viewName.asCatalogSchemaTableName()));
    }

    @Override
    public void checkCanRenameView(SecurityContext securityContext, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(viewName, "viewName is null");
        requireNonNull(newViewName, "newViewName is null");

        checkCanAccessCatalog(securityContext, viewName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanRenameView(securityContext.toSystemSecurityContext(), viewName.asCatalogSchemaTableName(), newViewName.asCatalogSchemaTableName()));
    }

    @Override
    public void checkCanDropView(SecurityContext securityContext, QualifiedObjectName viewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(viewName, "viewName is null");

        checkCanAccessCatalog(securityContext, viewName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanDropView(securityContext.toSystemSecurityContext(), viewName.asCatalogSchemaTableName()));
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SecurityContext securityContext, QualifiedObjectName tableName, Set<String> columnNames)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanCreateViewWithSelectFromColumns(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), columnNames));
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SecurityContext securityContext, String functionName, Identity grantee, boolean grantOption)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(functionName, "functionName is null");

        systemAuthorizationCheck(control -> control.checkCanGrantExecuteFunctionPrivilege(
                securityContext.toSystemSecurityContext(),
                functionName,
                new PrestoPrincipal(PrincipalType.USER, grantee.getUser()),
                grantOption));
    }

    @Override
    public void checkCanGrantTablePrivilege(SecurityContext securityContext, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal grantee, boolean grantOption)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanGrantTablePrivilege(securityContext.toSystemSecurityContext(), privilege, tableName.asCatalogSchemaTableName(), grantee, grantOption));
    }

    @Override
    public void checkCanRevokeTablePrivilege(SecurityContext securityContext, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal revokee, boolean grantOption)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanRevokeTablePrivilege(securityContext.toSystemSecurityContext(), privilege, tableName.asCatalogSchemaTableName(), revokee, grantOption));
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(propertyName, "propertyName is null");

        systemAuthorizationCheck(control -> control.checkCanSetSystemSessionProperty(new SystemSecurityContext(identity, Optional.empty()), propertyName));
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SecurityContext securityContext, String catalogName, String propertyName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(propertyName, "propertyName is null");

        checkCanAccessCatalog(securityContext, catalogName);

        systemAuthorizationCheck(control -> control.checkCanSetCatalogSessionProperty(securityContext.toSystemSecurityContext(), catalogName, propertyName));
    }

    @Override
    public void checkCanSelectFromColumns(SecurityContext securityContext, QualifiedObjectName tableName, Set<String> columnNames)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columnNames, "columnNames is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanSelectFromColumns(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), columnNames));
    }

    @Override
    public void checkCanCreateRole(SecurityContext securityContext, String role, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(role, "role is null");
        requireNonNull(grantor, "grantor is null");
        requireNonNull(catalogName, "catalogName is null");

        checkCanAccessCatalog(securityContext, catalogName);
    }

    @Override
    public void checkCanDropRole(SecurityContext securityContext, String role, String catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(role, "role is null");
        requireNonNull(catalogName, "catalogName is null");

        checkCanAccessCatalog(securityContext, catalogName);
    }

    @Override
    public void checkCanGrantRoles(SecurityContext securityContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(roles, "roles is null");
        requireNonNull(grantees, "grantees is null");
        requireNonNull(grantor, "grantor is null");
        requireNonNull(catalogName, "catalogName is null");

        checkCanAccessCatalog(securityContext, catalogName);
    }

    @Override
    public void checkCanRevokeRoles(SecurityContext securityContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(roles, "roles is null");
        requireNonNull(grantees, "grantees is null");
        requireNonNull(grantor, "grantor is null");
        requireNonNull(catalogName, "catalogName is null");

        checkCanAccessCatalog(securityContext, catalogName);
    }

    @Override
    public void checkCanSetRole(SecurityContext securityContext, String role, String catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(role, "role is null");
        requireNonNull(catalogName, "catalog is null");

        checkCanAccessCatalog(securityContext, catalogName);
    }

    @Override
    public void checkCanShowRoles(SecurityContext securityContext, String catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");

        checkCanAccessCatalog(securityContext, catalogName);
    }

    @Override
    public void checkCanShowCurrentRoles(SecurityContext securityContext, String catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");

        checkCanAccessCatalog(securityContext, catalogName);
    }

    @Override
    public void checkCanShowRoleGrants(SecurityContext securityContext, String catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");

        checkCanAccessCatalog(securityContext, catalogName);
    }

    @Override
    public void checkCanExecuteProcedure(SecurityContext securityContext, QualifiedObjectName procedureName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(procedureName, "procedureName is null");

        checkCanAccessCatalog(securityContext, procedureName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanExecuteProcedure(securityContext.toSystemSecurityContext(), procedureName.asCatalogSchemaRoutineName()));
    }

    @Override
    public void checkCanExecuteFunction(SecurityContext context, String functionName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(functionName, "functionName is null");

        systemAuthorizationCheck(control -> control.checkCanExecuteFunction(context.toSystemSecurityContext(), functionName));
    }

    @Override
    public List<ViewExpression> getRowFilters(SecurityContext context, QualifiedObjectName tableName)
    {
        requireNonNull(context, "securityContext is null");
        requireNonNull(tableName, "catalogName is null");

        ImmutableList.Builder<ViewExpression> filters = ImmutableList.builder();

        for (SystemAccessControl systemAccessControl : systemAccessControls.get()) {
            systemAccessControl.getRowFilter(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName())
                    .ifPresent(filters::add);
        }

        return filters.build();
    }

    @Override
    public List<ViewExpression> getColumnMasks(SecurityContext context, QualifiedObjectName tableName, String columnName, Type type)
    {
        requireNonNull(context, "securityContext is null");
        requireNonNull(tableName, "catalogName is null");

        ImmutableList.Builder<ViewExpression> masks = ImmutableList.builder();

        for (SystemAccessControl systemAccessControl : systemAccessControls.get()) {
            systemAccessControl.getColumnMask(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), columnName, type)
                    .ifPresent(masks::add);
        }

        return masks.build();
    }

    @Managed
    @Nested
    public CounterStat getAuthorizationSuccess()
    {
        return authorizationSuccess;
    }

    @Managed
    @Nested
    public CounterStat getAuthorizationFail()
    {
        return authorizationFail;
    }

    private void checkCanAccessCatalog(SecurityContext securityContext, String catalogName)
    {
        try {
            for (SystemAccessControl systemAccessControl : systemAccessControls.get()) {
                systemAccessControl.checkCanAccessCatalog(securityContext.toSystemSecurityContext(), catalogName);
            }
            authorizationSuccess.update(1);
        }
        catch (PrestoException e) {
            authorizationFail.update(1);
            throw e;
        }
    }

    // This is for the gateway case
    // once we start parsing the query we can remove this
    public void checkCanAccessCatalog(SystemSecurityContext securityContext, String catalogName)
    {
        try {
            for (SystemAccessControl systemAccessControl : systemAccessControls.get()) {
                systemAccessControl.checkCanAccessCatalog(securityContext, catalogName);
            }
            authorizationSuccess.update(1);
        }
        catch (PrestoException e) {
            authorizationFail.update(1);
            throw e;
        }
    }

    private void systemAuthorizationCheck(Consumer<SystemAccessControl> check)
    {
        try {
            for (SystemAccessControl systemAccessControl : systemAccessControls.get()) {
                check.accept(systemAccessControl);
            }
            authorizationSuccess.update(1);
        }
        catch (PrestoException e) {
            authorizationFail.update(1);
            throw e;
        }
    }

    private class CatalogAccessControlEntry
    {
        private final CatalogName catalogName;
        private final ConnectorAccessControl accessControl;

        public CatalogAccessControlEntry(CatalogName catalogName, ConnectorAccessControl accessControl)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
        }

        public CatalogName getCatalogName()
        {
            return catalogName;
        }

        public ConnectorAccessControl getAccessControl()
        {
            return accessControl;
        }
    }

    private static class InitializingSystemAccessControl
            extends ForwardingSystemAccessControl
    {
        @Override
        protected SystemAccessControl delegate()
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }
    }
}
