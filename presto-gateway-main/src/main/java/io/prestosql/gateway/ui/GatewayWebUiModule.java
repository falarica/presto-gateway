package io.prestosql.gateway.ui;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.prestosql.gateway.routing.RoutingManager;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;

public class GatewayWebUiModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        Logger log = Logger.get(RoutingManager.class);
        // UI not built
        if (getClass().getResource("/gatewaywebapp/index.html") == null) {
            httpServerBinder(binder).bindResource("/ui", "dummyuiapp").withWelcomeFile("index.html");
            binder.bind(WebUiAuthenticationManager.class).to(DisabledWebUiAuthenticationManager.class).in(Scopes.SINGLETON);
            return;
        }

        httpServerBinder(binder).bindResource("/ui", "gatewaywebapp").withWelcomeFile("index.html");

        configBinder(binder).bindConfig(WebUiConfig.class);
        // Web UI authentication is tied with various authenticators. Gateway will use the same authenticators.
        if (buildConfigObject(WebUiConfig.class).isEnabled()) {
            install(new WebUiAuthenticationModule());
        }
        else {
            binder.bind(WebUiAuthenticationManager.class).to(DisabledWebUiAuthenticationManager.class).in(Scopes.SINGLETON);
        }
    }
}
