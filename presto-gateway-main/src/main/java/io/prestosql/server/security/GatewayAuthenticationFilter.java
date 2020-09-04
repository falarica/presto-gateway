package io.prestosql.server.security;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.prestosql.gateway.ui.WebUiAuthenticationManager;
import io.prestosql.spi.security.Identity;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static io.prestosql.server.ServletSecurityUtils.sendErrorMessage;
import static io.prestosql.server.ServletSecurityUtils.skipRequestBody;
import static io.prestosql.server.ServletSecurityUtils.withAuthenticatedIdentity;
import static io.prestosql.server.security.BasicAuthCredentials.extractBasicAuthCredentials;
import static java.util.Objects.requireNonNull;
import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;

public class GatewayAuthenticationFilter
        implements Filter
{
    private final List<Authenticator> authenticators;
    private final WebUiAuthenticationManager uiAuthenticationManager;

    @Inject
    public GatewayAuthenticationFilter(List<Authenticator> authenticators,
                                SecurityConfig securityConfig,
                                WebUiAuthenticationManager uiAuthenticationManager)
    {
        this.authenticators = ImmutableList.copyOf(requireNonNull(authenticators, "authenticators is null"));
        this.uiAuthenticationManager = requireNonNull(uiAuthenticationManager, "uiAuthenticationManager is null");
    }

    @Override
    public void init(FilterConfig filterConfig) {}

    @Override
    public void destroy() {}

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain nextFilter)
            throws IOException, ServletException
    {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        if (WebUiAuthenticationManager.isUiRequest(request)) {
            uiAuthenticationManager.handleUiRequest(request, response, nextFilter);
            return;
        }

        // skip authentication if non-secure or not configured
        if (!doesRequestSupportAuthentication(request)) {
            handleInsecureRequest(nextFilter, request, response);
            return;
        }

        // try to authenticate, collecting errors and authentication headers
        Set<String> messages = new LinkedHashSet<>();
        Set<String> authenticateHeaders = new LinkedHashSet<>();

        for (Authenticator authenticator : authenticators) {
            Identity authenticatedIdentity;
            try {
                authenticatedIdentity = authenticator.authenticate(request);
            }
            catch (AuthenticationException e) {
                if (e.getMessage() != null) {
                    messages.add(e.getMessage());
                }
                e.getAuthenticateHeader().ifPresent(authenticateHeaders::add);
                continue;
            }

            // authentication succeeded
            withAuthenticatedIdentity(nextFilter, request, response, authenticatedIdentity);
            return;
        }

        // authentication failed
        skipRequestBody(request);

        for (String value : authenticateHeaders) {
            response.addHeader(WWW_AUTHENTICATE, value);
        }

        if (messages.isEmpty()) {
            messages.add("Unauthorized");
        }

        // The error string is used by clients for exception messages and
        // is presented to the end user, thus it should be a single line.
        String error = Joiner.on(" | ").join(messages);
        sendErrorMessage(response, SC_UNAUTHORIZED, error);
    }

    private static void handleInsecureRequest(FilterChain nextFilter, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException
    {
        Optional<BasicAuthCredentials> basicAuthCredentials;
        try {
            basicAuthCredentials = extractBasicAuthCredentials(request);
        }
        catch (AuthenticationException e) {
            sendErrorMessage(response, SC_FORBIDDEN, e.getMessage());
            return;
        }

        if (basicAuthCredentials.isEmpty()) {
            nextFilter.doFilter(request, response);
            return;
        }

        if (basicAuthCredentials.get().getPassword().isPresent()) {
            sendErrorMessage(response, SC_FORBIDDEN, "Password not allowed for insecure request");
            return;
        }

        withAuthenticatedIdentity(nextFilter, request, response, Identity.ofUser(basicAuthCredentials.get().getUser()));
    }

    private boolean doesRequestSupportAuthentication(HttpServletRequest request)
    {
        return !authenticators.isEmpty() && request.isSecure();
    }
}
