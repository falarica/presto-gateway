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

import com.google.common.collect.ImmutableMap;
import io.prestosql.gateway.persistence.DataStoreConfig;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGatewayConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GatewayConfig.class)
                .setSharedSecretFile(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("gateway.shared-secret-file", "test.secret")
                .build();

        GatewayConfig expected = new GatewayConfig()
                .setSharedSecretFile(new File("test.secret"));
        assertFullMapping(properties, expected);

        Map<String, String> dprop = new ImmutableMap.Builder<String, String>()
                .put("hikariCPPropsFile", "hikaricp.properties")
                .put("driver", "com.driver")
                .put("jdbcUrl", "postgres://localhost")
                .put("maximumPoolSize", "10")
                .put("user", "user")
                .put("password", "password")
                .put("poolIdleTimeout", "10")
                .put("poolConnectionTimeout", "10")
                .build();

        DataStoreConfig dconf = new DataStoreConfig().setHikariCPPropsFile("hikaricp.properties")
                .setDriver("com.driver")
                .setDatabaseUser("user")
                .setPassword("password")
                .setMaximumPoolSize(10)
                .setPoolConnectionTimeout(10)
                .setPoolIdleTimeout(10)
                .setJdbcUrl("postgres://localhost");
        assertFullMapping(dprop, dconf);
    }
}
