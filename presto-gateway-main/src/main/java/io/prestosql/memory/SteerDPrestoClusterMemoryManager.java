package io.prestosql.memory;

import io.prestosql.spi.memory.ClusterMemoryPoolManager;
import io.prestosql.spi.memory.MemoryPoolId;
import io.prestosql.spi.memory.MemoryPoolInfo;

import javax.inject.Inject;

import java.util.function.Consumer;

public class SteerDPrestoClusterMemoryManager
        implements ClusterMemoryPoolManager
{
    @Override
    public void addChangeListener(MemoryPoolId poolId, Consumer<MemoryPoolInfo> listener)
    {
    }

    @Inject
    public SteerDPrestoClusterMemoryManager() {}
}
