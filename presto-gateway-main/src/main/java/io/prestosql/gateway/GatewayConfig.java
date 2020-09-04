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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class GatewayConfig
{
    private File sharedSecretFile;
    private Duration clusterStatsPullInterval = new Duration(10, TimeUnit.SECONDS);
    private Duration queryHistoryCleanupInterval = new Duration(10, TimeUnit.MINUTES);
    private Duration transactionIdleCleanUpInterval = new Duration(10, TimeUnit.MINUTES);
    private int uiMaxQueryInfoListSize = 100;

    @MinDuration("1s")
    public Duration getClusterStatsPullInterval()
    {
        return clusterStatsPullInterval;
    }

    public Duration getQueryHistoryCleanupInterval()
    {
        return queryHistoryCleanupInterval;
    }

    public int getUiMaxQueryInfoListSize()
    {
        return uiMaxQueryInfoListSize;
    }

    @NotNull
    public File getSharedSecretFile()
    {
        return sharedSecretFile;
    }

    @Config("gateway.shared-secret-file")
    @ConfigDescription("Shared secret file used for authenticating URIs")
    public GatewayConfig setSharedSecretFile(File sharedSecretFile)
    {
        this.sharedSecretFile = sharedSecretFile;
        return this;
    }

    @Config("gateway.clusterstats-pull-interval")
    @ConfigDescription("Interval at which clusters stats are pulled for routing")
    public GatewayConfig setClusterStatsPullInterval(Duration clusterStatsPullInterval)
    {
        this.clusterStatsPullInterval = clusterStatsPullInterval;
        return this;
    }

    @Config("gateway.query-history-cleanup-interval")
    @ConfigDescription("Interval at which clusters stats are pulled for routing")
    public GatewayConfig setQueryHistoryCleanupInterval(Duration queryHistoryCleanupInterval)
    {
        this.queryHistoryCleanupInterval = queryHistoryCleanupInterval;
        return this;
    }

    @Config("gateway.ui-max-queryInfo-list-size")
    @ConfigDescription("Interval at which clusters stats are pulled for routing")
    public GatewayConfig setUiMaxQueryInfoListSize(int uiMaxQueryInfoListSize)
    {
        this.uiMaxQueryInfoListSize = uiMaxQueryInfoListSize;
        return this;
    }
}
