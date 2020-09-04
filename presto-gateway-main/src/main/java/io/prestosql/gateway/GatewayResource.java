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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.SessionRepresentation;
import io.prestosql.client.PrestoHeaders;
import io.prestosql.client.QueryError;
import io.prestosql.client.QueryResults;
import io.prestosql.client.StatementStats;
import io.prestosql.dispatcher.CoordinatorLocation;
import io.prestosql.dispatcher.DispatchExecutor;
import io.prestosql.dispatcher.DispatchInfo;
import io.prestosql.dispatcher.RoutedCoordinatorLocation;
import io.prestosql.execution.ExecutionFailureInfo;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.QueryStats;
import io.prestosql.gateway.GatewayResponseHandler.ProxyResponse;
import io.prestosql.gateway.persistence.QueryDetail;
import io.prestosql.gateway.persistence.QueryDetails2;
import io.prestosql.gateway.routing.RoutingManager;
import io.prestosql.security.AccessControl;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.BasicQueryStats;
import io.prestosql.server.GatewayRequestSessionContext;
import io.prestosql.server.SessionContext;
import io.prestosql.server.protocol.Slug;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorType;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.memory.MemoryPoolId;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.security.GroupProvider;
import io.prestosql.spi.security.Identity;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.hash.Hashing.hmacSha256;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.COOKIE;
import static com.google.common.net.HttpHeaders.SET_COOKIE;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.prestosql.execution.QueryState.FAILED;
import static io.prestosql.execution.QueryState.QUEUED;
import static io.prestosql.server.HttpRequestSessionContext.AUTHENTICATED_IDENTITY;
import static io.prestosql.server.protocol.Slug.Context.QUEUED_QUERY;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readAllBytes;
import static java.util.Collections.list;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_GATEWAY;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.noContent;

@Path("/")
public class GatewayResource
{
    private static final Logger log = Logger.get(GatewayResource.class);

    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();
    private static final Duration NO_DURATION = new Duration(0, MILLISECONDS);

    private static final String X509_ATTRIBUTE = "javax.servlet.request.X509Certificate";
    private static final Duration ASYNC_TIMEOUT = new Duration(2, MINUTES);
    private static final JsonFactory JSON_FACTORY = new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("proxy-%s"));
    private final HttpClient httpClient;
    private final RoutingManager routingManager;
    private final QueryHistoryManager queryHistoryManager;
    private final JsonWebTokenHandler jwtHandler;
    private final HashFunction hmac;
    private final GroupProvider groupProvider;

    private final AccessControl accessControl;

    private final SteerDDispatchManager dispatchManager;

    private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> routedClusterQueridToGatewayQueryId = new ConcurrentHashMap<>();

    private final ConcurrentMap<QueryId, Request.Builder> queriesRequestMap = new ConcurrentHashMap<>();

    private final ScheduledExecutorService timeoutExecutor;

    @Inject
    public GatewayResource(@ForGateway HttpClient httpClient, JsonWebTokenHandler jwtHandler,
            GatewayConfig config, RoutingManager router, QueryHistoryManager queryHistMgr,
            GroupProvider groupProvider,
            SteerDDispatchManager dispatchManager,
            DispatchExecutor executor,
            AccessControl accessControl)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.jwtHandler = requireNonNull(jwtHandler, "jwtHandler is null");
        this.hmac = hmacSha256(loadSharedSecret(config.getSharedSecretFile()));
        this.routingManager = router;
        this.queryHistoryManager = requireNonNull(queryHistMgr, "queryHistoryManager is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.timeoutExecutor = requireNonNull(executor, "timeoutExecutor is null").getScheduledExecutor();
        this.accessControl = requireNonNull(accessControl, "accessontrol was null");
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    // TODO: Suranjan Get info of all the clusters and provide it.
    @GET
    @Path("/v1/info")
    @Produces(APPLICATION_JSON)
    public void getInfo(
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders,
            @Suspended AsyncResponse asyncResponse)
    {
        String remoteAddress = servletRequest.getRemoteAddr();
        Optional<Identity> identity = Optional.ofNullable((Identity) servletRequest.getAttribute(AUTHENTICATED_IDENTITY));

        MultivaluedMap<String, String> headers = httpHeaders.getRequestHeaders();
        GatewayRequestSessionContext gatewayRequestContext = new GatewayRequestSessionContext(headers, remoteAddress, identity, groupProvider, "");
        URI routedURI = routingManager.providePrestoCluster(gatewayRequestContext, Optional.empty());

        Request.Builder request = prepareGet()
                .setUri(uriBuilderFrom(routedURI).replacePath("/v1/info").build());

        performRequest(servletRequest, asyncResponse, request, response ->
                responseWithHeaders(Response.ok(response.getBody()), response));
    }

    @GET
    @Path("/v1/statement/queued/{queryId}/{slug}/{token}")
    @Produces(APPLICATION_JSON)
    public void getStatus(
            @PathParam("queryId") QueryId queryId,
            @PathParam("slug") String slug,
            @PathParam("token") long token,
            @QueryParam("maxWait") Duration maxWait,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        Query query = getQuery(queryId, slug, token);
        QueryResults results = query.getQueryResults(query.getLastToken(), uriInfo);

        URI routedURI = results.getNextUri();

        // If it is queued then return immediately.
        if (routedURI.toASCIIString().contains("queued")) {
            setupAsyncResponse(asyncResponse, immediateFuture(Response.ok(results).build()));
        }
        else {
            String remoteAddress = servletRequest.getRemoteAddr();
            Optional<Identity> identity = Optional.ofNullable((Identity) servletRequest.getAttribute(AUTHENTICATED_IDENTITY));
            MultivaluedMap<String, String> headers = httpHeaders.getRequestHeaders();

            GatewayRequestSessionContext gatewayRequestContext = new GatewayRequestSessionContext(headers, remoteAddress,
                    identity, groupProvider, query.query);
            String source = gatewayRequestContext.getSource();
            String usr = gatewayRequestContext.getIdentity().getUser();
            log.info("GetStatus The session schema is %s", gatewayRequestContext.getSchema());

            if (identity.isPresent()) {
                usr = identity.get().getUser();
                // String user = QueryRoutingContextProvider.getSessionUser(servletRequest);
                log.debug("The user is %s", usr);
            }
            final String user = usr;
            log.debug("getStatus uriInfo path %s and routedPath is %s", uriInfo.getAbsolutePath(), routedURI.toASCIIString());
            Request.Builder requestBuilder = queriesRequestMap.get(queryId);

            requestBuilder
                    .setUri(uriBuilderFrom(routedURI).replacePath("/v1/statement").build())
                    .setBodyGenerator(createStaticBodyGenerator(query.query, UTF_8));

            Request request = requestBuilder
                    .setPreserveAuthorizationOnRedirect(true)
                    .build();

            // FOR DDL with state assume that it is SUCCESSFUL in each of the cluster.
            // if it fails in some, we need to retry?Fail? Throw exception to the user?

            performRequest(request, asyncResponse,
                    response -> buildResponse(uriInfo, response,
                            Optional.of((queryIdarg, txId) -> cacheQueryDetails(query.getQueryId(), queryIdarg, query.query, user, source, routedURI.toASCIIString(), txId))));
        }
    }

    @POST
    @Path("/v1/statement")
    @Produces(APPLICATION_JSON)
    public void postStatement(
            String statement,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        if (isNullOrEmpty(statement)) {
            throw badRequest(BAD_REQUEST, "SQL statement is empty");
        }
        String remoteAddress = servletRequest.getRemoteAddr();
        Optional<Identity> identity = Optional.ofNullable((Identity) servletRequest.getAttribute(AUTHENTICATED_IDENTITY));
        MultivaluedMap<String, String> headers = httpHeaders.getRequestHeaders();
        GatewayRequestSessionContext gatewayRequestContext = new GatewayRequestSessionContext(headers, remoteAddress,
                identity, groupProvider, statement);

        Query query = new Query(statement, gatewayRequestContext, dispatchManager);
        queries.put(query.getQueryId(), query);
        // TODO: Provide the URI from the co-ordinator URI.
        try {
            dispatchManager.dispatchQuery(query.getQueryId(), query.getSlug(), gatewayRequestContext, statement);
        }
        catch (URISyntaxException e) {
            log.error(e, "URI format is not correct. Please check the cluster url");
            throw new GatewayException(e);
        }

        QueryResults results = query.getQueryResults(query.getLastToken(), uriInfo);
        if (results.getStats().getState().equals(FAILED.toString())) {
            setupAsyncResponse(asyncResponse, immediateFuture(Response.ok(query.getQueryResults(query.getLastToken(), uriInfo)).build()));
            return;
        }
        // Duration maxWait = new Duration(1, SECONDS);
        // query.waitForDispatched();
        // wait for dispatched or queued on the gateway.

        URI routedURI = results.getNextUri();
        // If it is queued then return immediately.

        //TODO: if nextUri is null then the query has failed to be queued too.
        // we need to throw an exception to user.
        Request.Builder requestBuilder = preparePost();
        createRequestBuilder(servletRequest, requestBuilder);

        requestBuilder.setUri(uriBuilderFrom(routedURI).replacePath("/v1/statement").build())
                .setBodyGenerator(createStaticBodyGenerator(statement, UTF_8));

        Request request = createRequest(servletRequest, requestBuilder);

        if (routedURI.toASCIIString().contains("queued")) {
            //save the request object to be used next time.
            queriesRequestMap.put(query.queryId, requestBuilder);
            setupAsyncResponse(asyncResponse, immediateFuture(Response.ok(results).build()));
        }
        else {
            String source = gatewayRequestContext.getSource();
            String usr = gatewayRequestContext.getIdentity().getUser();
            if (identity.isPresent()) {
                usr = identity.get().getUser();
            }
            final String user = usr;
            // FOR DDL with state assume that it is SUCCESSFUL in each of the cluster.
            // if it fails in some, we need to retry?Fail? Throw exception to the user?

            performRequest(request, asyncResponse,
                    response -> buildResponse(uriInfo, response,
                            Optional.of((queryIdarg, txId) -> cacheQueryDetails(query.getQueryId(), queryIdarg, query.query, user, source, routedURI.toASCIIString(), txId))));
        }
    }

    // In case we queue it, we need to send them the nextUri as queued
    // so that they can keep sending the response and show the status as queued.
    // but we don't have the queryid and other details.
    // maybe we can generate the queryid? temporarily which again can be changed later when
    // we actually submit the query.

    @GET
    @Path("/v1/proxy")
    @Produces(APPLICATION_JSON)
    public void getNext(
            @QueryParam("uri") String uri,
            @QueryParam("hmac") String hash,
            @Context HttpServletRequest servletRequest,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        if (!hmac.hashString(uri, UTF_8).equals(HashCode.fromString(hash))) {
            throw badRequest(FORBIDDEN, "Failed to validate HMAC of URI");
        }
        URI clusterURI = URI.create(uri);
        Request.Builder request = prepareGet().setUri(clusterURI);

        String clusterURL = getClusterURLFromURIString(clusterURI);
        String queryId = getQueryIdFromURIString(uri);

        performRequest(servletRequest, asyncResponse, request, response -> buildResponse(uriInfo, response,
                Optional.of((updateTime, txId) -> storeQueryStats(queryId, clusterURL, txId, updateTime))));
    }

    private String getClusterURLFromURIString(URI uri)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(uri.getScheme());
        sb.append("://");
        sb.append(uri.getHost());
        sb.append(":");
        sb.append(uri.getPort());

        return sb.toString();
    }

    @DELETE
    @Path("/v1/proxy")
    @Produces(APPLICATION_JSON)
    public void cancelQuery(
            @QueryParam("uri") String uri,
            @QueryParam("hmac") String hash,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
    {
        if (!hmac.hashString(uri, UTF_8).equals(HashCode.fromString(hash))) {
            throw badRequest(FORBIDDEN, "Failed to validate HMAC of URI");
        }
        // if someone is coming on v1/proxy it means he is already authenticated through v1/stmt
        Request.Builder request = prepareDelete().setUri(URI.create(uri));
        performRequest(servletRequest, asyncResponse, request, response -> responseWithHeaders(noContent(), response));
    }

    @Path("/query")
    @GET
    public List<BasicQueryInfo> getAllQueryInfo(@QueryParam("state") String stateFilter, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        QueryState expectedState = stateFilter == null ? null : QueryState.valueOf(stateFilter.toUpperCase(Locale.ENGLISH));

        List<QueryDetails2> queries = dispatchManager.getQueries();
        ImmutableList.Builder<BasicQueryInfo> builder = new ImmutableList.Builder<>();

        ObjectMapper mapper = new ObjectMapper();
        JsonCodec<QueryStats> codec = JsonCodec.jsonCodec(QueryStats.class);
        JsonCodec<QueryId> queryIdCodec = JsonCodec.jsonCodec(QueryId.class);
        JsonCodec<SessionRepresentation> sessionCoded = JsonCodec.jsonCodec(SessionRepresentation.class);
        JsonCodec<ResourceGroupId> resourceGroupIdCodec = JsonCodec.jsonCodec(ResourceGroupId.class);

        JsonCodec<QueryState> queryStateCodec = JsonCodec.jsonCodec(QueryState.class);
        JsonCodec<MemoryPoolId> memPoolCodec = JsonCodec.jsonCodec(MemoryPoolId.class);

        JsonCodec<ErrorType> errTypecodec = JsonCodec.jsonCodec(ErrorType.class);
        JsonCodec<ErrorCode> errCodeCodec = JsonCodec.jsonCodec(ErrorCode.class);

        for (QueryDetails2 queryDetail : queries) {
            String queryInfo = ""; //queryDetail.getPrestoQueryInfo();
            JsonNode json = null;
            try {
                json = mapper.readTree(queryInfo);
                JsonNode queryStats = json.get("queryStats");
                JsonNode queryId = json.get("queryId");
                JsonNode sessionRep = json.get("session");
                JsonNode resourceGroupId = json.get("resourceGroupId");
                JsonNode state = json.get("state");
                JsonNode memPool = json.get("memoryPool");
                JsonNode scheduled = json.get("scheduled");
                JsonNode uri = json.get("self");
                JsonNode query = json.get("query");
                JsonNode updateType = json.get("updateType");
                JsonNode preparedQuery = json.get("preparedQuery");
                JsonNode errorType = json.get("errorType");
                JsonNode errorCode = json.get("errorCode");
                BasicQueryStats stats = new BasicQueryStats(codec.fromJson(queryStats.toString()));

                BasicQueryInfo bqInfo = new BasicQueryInfo(
                        queryIdCodec.fromJson(queryId.toString()),
                        sessionCoded.fromJson(sessionRep.toString()),
                        resourceGroupId == null ? Optional.empty() : Optional.of(resourceGroupIdCodec.fromJson(resourceGroupId.toString())),
                        queryStateCodec.fromJson(state.toString()),
                        memPool == null ? null : memPoolCodec.fromJson(memPool.toString()),
                        scheduled.asBoolean(),
                        mapper.readValue(uri.toString(), URI.class),
                        query.asText(),
                        updateType == null ? Optional.empty() : Optional.of(updateType.asText()),
                        preparedQuery == null ? Optional.empty() : Optional.of(preparedQuery.asText()),
                        stats,
                        errorType == null ? null : errTypecodec.fromJson(errorType.toString()),
                        errorCode == null ? null : errCodeCodec.fromJson(errorCode.toString()));
                if (stateFilter == null || state.asText().equalsIgnoreCase(expectedState.toString())) {
                    builder.add(bqInfo);
                }
            }
            catch (JsonProcessingException e) {
                log.warn(e, "Couldn't parse the JSON");
            }
        }
        return builder.build();
    }

    @Path("/querydetails")
    @GET
    public List<QueryDetails2> getAllQueryDetails(@QueryParam("state") String stateFilter, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        QueryState expectedState = stateFilter == null ? null : QueryState.valueOf(stateFilter.toUpperCase(Locale.ENGLISH));
        List<QueryDetails2> queries = dispatchManager.getQueries();
        return queries;
    }

    @Path("/querydetails2")
    @GET
    public List<QueryDetails2> getAllQueryDetails2(@QueryParam("state") String stateFilter, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        return dispatchManager.getQueries();
    }

    @Path("/queryfullinfo")
    @GET
    public List<QueryDetails2> getAllBasicQueryInfo(@QueryParam("state") String stateFilter,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders,
            @Suspended AsyncResponse asyncResponse)
    {
        return dispatchManager.getQueries();
    }

    private Query getQuery(QueryId queryId, String slug, long token)
    {
        Query query = queries.get(queryId);
        if (query == null || !query.getSlug().isValid(QUEUED_QUERY, slug, token)) {
            throw badRequest(NOT_FOUND, "Query not found");
        }
        return query;
    }

    private void createRequestBuilder(HttpServletRequest servletRequest, Request.Builder requestBuilder)
    {
        setupBearerToken(servletRequest, requestBuilder);
        for (String name : list(servletRequest.getHeaderNames())) {
            if (isPrestoHeader(name) || name.equalsIgnoreCase(COOKIE)) {
                for (String value : list(servletRequest.getHeaders(name))) {
                    requestBuilder.addHeader(name, value);
                }
            }
            else if (name.equalsIgnoreCase(USER_AGENT)) {
                for (String value : list(servletRequest.getHeaders(name))) {
                    requestBuilder.addHeader(name, "[Presto Proxy] " + value);
                }
            }
        }
    }

    private Request createRequest(HttpServletRequest servletRequest, Request.Builder requestBuilder)
    {
        setupBearerToken(servletRequest, requestBuilder);
        for (String name : list(servletRequest.getHeaderNames())) {
            if (isPrestoHeader(name) || name.equalsIgnoreCase(COOKIE)) {
                for (String value : list(servletRequest.getHeaders(name))) {
                    requestBuilder.addHeader(name, value);
                }
            }
            else if (name.equalsIgnoreCase(USER_AGENT)) {
                for (String value : list(servletRequest.getHeaders(name))) {
                    requestBuilder.addHeader(name, "[Presto Proxy] " + value);
                }
            }
        }

        Request request = requestBuilder
                .setPreserveAuthorizationOnRedirect(true)
                .build();

        return request;
    }

    private void performRequest(
            Request request,
            AsyncResponse asyncResponse,
            Function<ProxyResponse, Response> responseBuilder)
    {
        ListenableFuture<Response> future = executeHttp(request)
                .transform(responseBuilder::apply, executor)
                .catching(GatewayException.class, e -> handleProxyException(request, e), directExecutor());

        setupAsyncResponse(asyncResponse, future);
    }

    private void performRequest(
            HttpServletRequest servletRequest,
            AsyncResponse asyncResponse,
            Request.Builder requestBuilder,
            Function<ProxyResponse, Response> responseBuilder)
    {
        setupBearerToken(servletRequest, requestBuilder);
        for (String name : list(servletRequest.getHeaderNames())) {
            if (isPrestoHeader(name) || name.equalsIgnoreCase(COOKIE)) {
                for (String value : list(servletRequest.getHeaders(name))) {
                    requestBuilder.addHeader(name, value);
                }
            }
            else if (name.equalsIgnoreCase(USER_AGENT)) {
                for (String value : list(servletRequest.getHeaders(name))) {
                    requestBuilder.addHeader(name, "[Presto Proxy] " + value);
                }
            }
        }

        Request request = requestBuilder
                .setPreserveAuthorizationOnRedirect(true)
                .build();

        ListenableFuture<Response> future = executeHttp(request)
                .transform(responseBuilder::apply, executor)
                .catching(GatewayException.class, e -> handleProxyException(request, e), directExecutor());

        setupAsyncResponse(asyncResponse, future);
    }

    // TODO: make it async
    private Void storeQueryStats(String queryId,
            String clusterURL,
            String txId,
            String updateTime)
    {
        QueryId gatewayQueryId = new QueryId(this.routedClusterQueridToGatewayQueryId.get(queryId));
        // need to update the heartbeat so that query are cleaned regularly for dead clients.
        if (!updateTime.isEmpty()) {
            // update the last Heartbeat
            this.dispatchManager.updateHeartbeat(gatewayQueryId);
            return (Void) null;
        }
        log.info("Going to store the queryInfo for queryid " + queryId);
        try {
            String qInfo = this.queryHistoryManager.scheduleStatsFetchForCompletedQuery(new QueryDetail(queryId,
                    null, null, null,
                    clusterURL, System.currentTimeMillis(), null));

            this.dispatchManager.queryFinished(gatewayQueryId, qInfo);
            this.routedClusterQueridToGatewayQueryId.remove(queryId);
            if (txId.startsWith("started")) {
                String[] txIds = txId.split(":");
                this.routingManager.addTxIdToRoutedCluster(txIds[1], clusterURL);
            }
            else if (txId.startsWith("clear")) {
                String[] txIds = txId.split(":");
                this.routingManager.removeTxIdToRoutedCluster(txIds[1]);
            }
        }
        catch (UnsupportedEncodingException e) {
            throw new GatewayException(e);
        }
        return (Void) null;
    }

    private Void cacheQueryDetails(QueryId gatewayQueryId, String queryId,
            String sql,
            String user,
            String source,
            String clusterURL,
            String txId)
    {
        this.routedClusterQueridToGatewayQueryId.put(queryId, gatewayQueryId.toString());
        // make it async.
        Optional<ResourceGroupId> rsId = this.dispatchManager.getResourceGroupForQuery(gatewayQueryId);
        this.queryHistoryManager.submitRunningQueryDetail(new QueryDetail(queryId,
                sql, user, source,
                clusterURL, System.currentTimeMillis(), null));
        this.routingManager.addTxIdToRoutedCluster(txId, clusterURL);
        return (Void) null;
    }

    private Response buildResponse(UriInfo uriInfo, ProxyResponse response, Optional<BiFunction<String, String, Void>> queryCacher)
    {
        byte[] body = rewriteResponse(uriInfo, response, uri -> rewriteUri(uriInfo, uri), queryCacher);
        return responseWithHeaders(Response.ok(body), response);
    }

    private String rewriteUri(UriInfo uriInfo, String uri)
    {
        String absolutePath = uriInfo.getAbsolutePathBuilder()
                .replacePath("/v1/proxy")
                .queryParam("uri", uri)
                .queryParam("hmac", hmac.hashString(uri, UTF_8))
                .build()
                .toString();
        return absolutePath;
    }

    private void setupAsyncResponse(AsyncResponse asyncResponse, ListenableFuture<Response> future)
    {
        bindAsyncResponse(asyncResponse, future, executor)
                .withTimeout(ASYNC_TIMEOUT, () -> Response
                        .status(BAD_GATEWAY)
                        .type(TEXT_PLAIN_TYPE)
                        .entity("Request to remote Presto server timed out after" + ASYNC_TIMEOUT)
                        .build());
    }

    private FluentFuture<ProxyResponse> executeHttp(Request request)
    {
        return FluentFuture.from(httpClient.executeAsync(request, new GatewayResponseHandler()));
    }

    private void setupBearerToken(HttpServletRequest servletRequest, Request.Builder requestBuilder)
    {
        if (!jwtHandler.isConfigured()) {
            for (String name : list(servletRequest.getHeaderNames())) {
                if (name.equalsIgnoreCase(AUTHORIZATION)) {
                    for (String value : list(servletRequest.getHeaders(name))) {
                        requestBuilder.addHeader(name, value);
                    }
                }
            }
            return;
        }

        X509Certificate[] certs = (X509Certificate[]) servletRequest.getAttribute(X509_ATTRIBUTE);
        if ((certs == null) || (certs.length == 0)) {
            throw badRequest(FORBIDDEN, "No TLS certificate present for request");
        }
        String principal = certs[0].getSubjectX500Principal().getName();

        String accessToken = jwtHandler.getBearerToken(principal);
        requestBuilder.addHeader(AUTHORIZATION, "Bearer " + accessToken);
    }

    private static <T> T handleProxyException(Request request, GatewayException e)
    {
        log.warn(e, "Proxy request failed: %s %s", request.getMethod(), request.getUri());
        throw badRequest(BAD_GATEWAY, e.getMessage());
    }

    private static WebApplicationException badRequest(Status status, String message)
    {
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }

    private static boolean isPrestoHeader(String name)
    {
        return name.toLowerCase(ENGLISH).startsWith("x-presto-");
    }

    private static Response responseWithHeaders(ResponseBuilder builder, ProxyResponse response)
    {
        response.getHeaders().forEach((headerName, value) -> {
            String name = headerName.toString();
            if (name.equalsIgnoreCase(PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID)) {
                log.debug("CLEAR TX ID is " + value);
            }
            if (name.equalsIgnoreCase(PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID)) {
                log.debug("STARTED TX ID is " + value);
            }
            if (isPrestoHeader(name) || name.equalsIgnoreCase(SET_COOKIE)) {
                builder.header(name, value);
            }
        });
        return builder.build();
    }

    private static byte[] rewriteResponse(UriInfo uriInfo, ProxyResponse response, Function<String, String> uriRewriter,
            Optional<BiFunction<String, String, Void>> queryCacher)
    {
        byte[] input = response.getBody();
        try {
            JsonParser parser = JSON_FACTORY.createParser(input);
            ByteArrayOutputStream out = new ByteArrayOutputStream(input.length * 2);
            JsonGenerator generator = JSON_FACTORY.createGenerator(out);

            JsonToken token = parser.nextToken();
            if (token != START_OBJECT) {
                throw invalidJson("bad start token: " + token);
            }
            generator.copyCurrentEvent(parser);

            boolean isNextUriNull = true;
            while (true) {
                token = parser.nextToken();
                if (token == null) {
                    throw invalidJson("unexpected end of stream");
                }

                if (token == END_OBJECT) {
                    generator.copyCurrentEvent(parser);
                    break;
                }

                if (token == FIELD_NAME) {
                    String name = parser.getValueAsString();
                    // Changes the nextURI
                    // TODO: In some cases, we may not want this if perf is an issue.
                    if (!"nextUri".equals(name) && !"partialCancelUri".equals(name)) {
                        generator.copyCurrentStructure(parser);
                        continue;
                    }
                    isNextUriNull = false;
                    token = parser.nextToken();
                    if (token != VALUE_STRING) {
                        throw invalidJson(format("bad %s token: %s", name, token));
                    }
                    String value = parser.getValueAsString();
                    if (queryCacher.isPresent()) {
                        if (!(uriInfo.getAbsolutePath().toString().contains("proxy"))) {
                            // provide the txId if present
                            final String[] startedTxId = {""};
                            response.getHeaders().forEach((headerName, val) -> {
                                String hName = headerName.toString();
                                if (hName.equalsIgnoreCase(PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID)) {
                                    startedTxId[0] = val;
                                }
                            });
                            queryCacher.get().apply(getQueryIdFromURIString(value), startedTxId[0]);
                        }
                        else {
                            queryCacher.get().apply("heartbeat", "txID[0]");
                        }
                    }
                    value = uriRewriter.apply(value);
                    generator.writeStringField(name, value);
                    continue;
                }

                throw invalidJson("unexpected token: " + token);
            }
            if (isNextUriNull) {
                // query execution is complete.
                // we should scedule the query info collection task.
                // provide the clear txId
                final String[] txID = {""};
                response.getHeaders().forEach((headerName, val) -> {
                    String hName = headerName.toString();
                    if (hName.equalsIgnoreCase(PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID)) {
                        txID[0] = "started:" + val;
                    }
                    if (hName.equalsIgnoreCase(PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID)) {
                        txID[0] = "clear:" + val;
                    }
                });
                queryCacher.get().apply("", txID[0]);
                log.debug("This is the last result.");
            }
            token = parser.nextToken();
            if (token != null) {
                throw invalidJson("unexpected token after object close: " + token);
            }
            generator.close();

            return out.toByteArray();
        }
        catch (Throwable e) {
            throw new GatewayException(e);
        }
    }

    // TODO: Suranjan Find if there are other patterns.
    private static String getQueryIdFromURIString(String uriString)
    {
        String[] tokens = uriString.split("/");
        String queryId = tokens[6];
        return queryId;
    }

    private static IOException invalidJson(String message)
    {
        return new IOException("Invalid JSON response from remote Presto server: " + message);
    }

    private static byte[] loadSharedSecret(File file)
    {
        try {
            return Base64.getMimeDecoder().decode(readAllBytes(file.toPath()));
        }
        catch (IOException | IllegalArgumentException e) {
            throw new RuntimeException("Failed to load shared secret file: " + file, e);
        }
    }

    private static URI getQueryHtmlUri(QueryId queryId, UriInfo uriInfo)
    {
        return uriInfo.getRequestUriBuilder()
                .replacePath("ui/query.html")
                .replaceQuery(queryId.toString())
                .build();
    }

    private static URI getQueuedUri(QueryId queryId, Slug slug, long token, UriInfo uriInfo)
    {
        return uriInfo.getBaseUriBuilder()
                .replacePath("/v1/statement/queued/")
                .path(queryId.toString())
                .path(slug.makeSlug(QUEUED_QUERY, token))
                .path(String.valueOf(token))
                .replaceQuery("")
                .build();
    }

    private static QueryResults createQueryResults(
            QueryId queryId,
            URI nextUri,
            Optional<QueryError> queryError,
            UriInfo uriInfo,
            Duration elapsedTime,
            Duration queuedTime)
    {
        QueryState state = queryError.map(error -> FAILED).orElse(QUEUED);
        return new QueryResults(
                queryId.toString(),
                getQueryHtmlUri(queryId, uriInfo),
                null,
                nextUri,
                null,
                null,
                StatementStats.builder()
                        .setState(state.toString())
                        .setQueued(state == QUEUED)
                        .setElapsedTimeMillis(elapsedTime.toMillis())
                        .setQueuedTimeMillis(queuedTime.toMillis())
                        .build(),
                queryError.orElse(null),
                ImmutableList.of(),
                null,
                null);
    }

    private static final class Query
    {
        private final String query;
        private final SessionContext sessionContext;
        private final SteerDDispatchManager dispatchManager;
        private final QueryId queryId;
        private final Slug slug = Slug.createNew();
        private final AtomicLong lastToken = new AtomicLong();

        public Query(String query, SessionContext sessionContext, SteerDDispatchManager dispatchManager)
        {
            this.query = requireNonNull(query, "query is null");
            this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
            this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
            this.queryId = dispatchManager.createQueryId();
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public Slug getSlug()
        {
            return slug;
        }

        public long getLastToken()
        {
            return lastToken.get();
        }

        public QueryResults getQueryResults(long token, UriInfo uriInfo)
        {
            long lastToken = this.lastToken.get();
            // token should be the last token or the next token
            if (token != lastToken && token != lastToken + 1) {
                throw new WebApplicationException(Response.Status.GONE);
            }
            // advance (or stay at) the token
            this.lastToken.compareAndSet(lastToken, token);

            Optional<DispatchInfo> dispatchInfo = dispatchManager.getDispatchInfo(queryId);
            if (dispatchInfo.isEmpty()) {
                // query should always be found, but it may have just been determined to be abandoned
                throw new WebApplicationException(Response
                        .status(NOT_FOUND)
                        .build());
            }

            return createQueryResults(token + 1, uriInfo, dispatchInfo.get());
        }

        public void destroy()
        {
            sessionContext.getIdentity().destroy();
        }

        private QueryResults createQueryResults(long token, UriInfo uriInfo, DispatchInfo dispatchInfo)
        {
            URI nextUri = getNextUri(token, uriInfo, dispatchInfo);

            Optional<QueryError> queryError = dispatchInfo.getFailureInfo()
                    .map(this::toQueryError);

            return GatewayResource.createQueryResults(
                    queryId,
                    nextUri,
                    queryError,
                    uriInfo,
                    dispatchInfo.getElapsedTime(),
                    dispatchInfo.getQueuedTime());
        }

        private URI getNextUri(long token, UriInfo uriInfo, DispatchInfo dispatchInfo)
        {
            // if failed, query is complete
            if (dispatchInfo.getFailureInfo().isPresent()) {
                log.debug("dispatchinfo is failure.. ");
                return null;
            }
            // if dispatched, redirect to new uri
            return dispatchInfo.getCoordinatorLocation()
                    .map(coordinatorLocation -> getRedirectUri(coordinatorLocation))
                    .orElseGet(() -> getQueuedUri(queryId, slug, token, uriInfo));
        }

        // here change the uri..to
        private URI getRedirectUri(CoordinatorLocation coordinatorLocation)
        {
            URI coordinatorUri = ((RoutedCoordinatorLocation) coordinatorLocation).getUri();
            return UriBuilder.fromUri(coordinatorUri)
                    .build();
        }

        private QueryError toQueryError(ExecutionFailureInfo executionFailureInfo)
        {
            ErrorCode errorCode;
            if (executionFailureInfo.getErrorCode() != null) {
                errorCode = executionFailureInfo.getErrorCode();
            }
            else {
                errorCode = GENERIC_INTERNAL_ERROR.toErrorCode();
                log.warn("Failed query %s has no error code", queryId);
            }

            return new QueryError(
                    firstNonNull(executionFailureInfo.getMessage(), "Internal error"),
                    null,
                    errorCode.getCode(),
                    errorCode.getName(),
                    errorCode.getType().toString(),
                    executionFailureInfo.getErrorLocation(),
                    executionFailureInfo.toFailureInfo());
        }
    }
}
