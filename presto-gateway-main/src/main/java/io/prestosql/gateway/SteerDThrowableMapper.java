package io.prestosql.gateway;

import com.google.common.base.Throwables;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import java.util.Optional;

import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Provider
public class SteerDThrowableMapper
        implements ExceptionMapper<Throwable>
{
    private static final Logger log = Logger.get(SteerDThrowableMapper.class);
    JsonCodec<ErrorInfo> codec = JsonCodec.jsonCodec(ErrorInfo.class);

    @Inject
    SteerDThrowableMapper()
    {
    }

    @Context
    private HttpServletRequest request;

    @Override
    public Response toResponse(Throwable throwable)
    {
        log.warn(throwable, "Request failed for %s", request.getRequestURI());

        if (throwable instanceof WebApplicationException) {
            Response.ResponseBuilder responseBuilder = Response.status(Response.Status.BAD_REQUEST)
                    .entity(responseJSON(throwable))
                    .header(CONTENT_TYPE, APPLICATION_JSON);
            return responseBuilder.build();
        }

        Response.ResponseBuilder responseBuilder = Response.serverError()
                .header(CONTENT_TYPE, APPLICATION_JSON);
        responseBuilder.entity(responseJSON(throwable));
        return responseBuilder.build();
    }

    private String responseJSON(final Throwable throwable)
    {
        ErrorInfo errorInfo = new ErrorInfo("Exception processing request",
                throwable.getMessage(),
                Optional.of(Throwables.getStackTraceAsString(throwable)));
        try {
            return codec.toJson(errorInfo);
        }
        catch (IllegalArgumentException e) {
            return "{\"message\":\"An internal error occurred\"}";
        }
    }
}
