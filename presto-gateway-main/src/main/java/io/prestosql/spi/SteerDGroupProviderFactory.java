package io.prestosql.spi;

import io.prestosql.spi.security.GroupProvider;
import io.prestosql.spi.security.GroupProviderFactory;

import java.util.Map;

public class SteerDGroupProviderFactory
        implements GroupProviderFactory
{
    @Override
    public String getName()
    {
        return "steerd-group-provider";
    }

    @Override
    public GroupProvider create(Map<String, String> config)
    {
        return new SteerDGroupProvider();
    }
}
