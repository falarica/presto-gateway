package io.prestosql.security;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.prestosql.plugin.base.util.LoggingInvocationHandler;
import io.prestosql.spi.security.GroupProvider;

import static com.google.common.reflect.Reflection.newProxy;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class SteerDAccessControlModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(AccessControlManager.class).to(SteerDAccessControlManager.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(AccessControlConfig.class);
        binder.bind(SteerDAccessControlManager.class).in(Scopes.SINGLETON);
        binder.bind(GroupProviderManager.class).in(Scopes.SINGLETON);
        binder.bind(GroupProvider.class).to(GroupProviderManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(SteerDAccessControlManager.class).withGeneratedName();
    }

    @Provides
    @Singleton
    public AccessControl createAccessControl(SteerDAccessControlManager accessControlManager)
    {
        Logger logger = Logger.get(AccessControl.class);

        AccessControl loggingInvocationsAccessControl = newProxy(
                AccessControl.class,
                new LoggingInvocationHandler(
                        accessControlManager,
                        new LoggingInvocationHandler.ReflectiveParameterNamesProvider(),
                        logger::debug));

        return ForwardingAccessControl.of(() -> {
            if (logger.isDebugEnabled()) {
                return loggingInvocationsAccessControl;
            }
            return accessControlManager;
        });
    }
}
