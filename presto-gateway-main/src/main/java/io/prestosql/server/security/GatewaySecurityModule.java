package io.prestosql.server.security;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.airlift.http.server.TheServlet;

import javax.servlet.Filter;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Locale.ENGLISH;

public class GatewaySecurityModule
        extends ServerSecurityModule
{
    @Override
    protected void setup(Binder binder)
    {
        newSetBinder(binder, Filter.class, TheServlet.class).addBinding()
                .to(GatewayAuthenticationFilter.class).in(Scopes.SINGLETON);
        binder.bind(PasswordAuthenticatorManager.class).in(Scopes.SINGLETON);

        authenticatorBinder(binder); // create empty map binder

        installAuthenticator("certificate", CertificateAuthenticator.class, CertificateConfig.class);
        installAuthenticator("kerberos", KerberosAuthenticator.class, KerberosConfig.class);
        installAuthenticator("password", PasswordAuthenticator.class, PasswordAuthenticatorConfig.class);
        installAuthenticator("jwt", JsonWebTokenAuthenticator.class, JsonWebTokenConfig.class);
    }

    private void installAuthenticator(String name, Class<? extends Authenticator> authenticator, Class<?> config)
    {
        install(authenticatorModule(name, authenticator, binder -> configBinder(binder).bindConfig(config)));
    }

    private static MapBinder<String, Authenticator> authenticatorBinder(Binder binder)
    {
        return newMapBinder(binder, String.class, Authenticator.class);
    }

    private static List<String> authenticationTypes(SecurityConfig config)
    {
        return config.getAuthenticationTypes().stream()
                .map(type -> type.toLowerCase(ENGLISH))
                .collect(toImmutableList());
    }
}
