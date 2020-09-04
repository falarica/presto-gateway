package io.prestosql.spi;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.spi.security.GroupProvider;

import java.util.Set;

public class SteerDGroupProvider
        implements GroupProvider
{
    private static final Logger log = Logger.get(SteerDGroupProvider.class);

    @Override
    public Set<String> getGroups(String user)
    {
        log.debug("Got called for the user %s", user);
        return ImmutableSet.of("group");
    }
}
