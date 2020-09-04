package io.prestosql.dispatcher;

import javax.ws.rs.core.UriInfo;

import java.net.URI;

public class RoutedCoordinatorLocation
        implements CoordinatorLocation
{
    private final URI uri;

    public RoutedCoordinatorLocation(URI uri)
    {
        this.uri = uri;
    }

    public URI getUri()
    {
        return uri;
    }

    @Override
    public URI getUri(UriInfo uriInfo)
    {
        return uriInfo.getRequestUriBuilder()
                .replacePath("")
                .replaceQuery("")
                .build();
    }
}
