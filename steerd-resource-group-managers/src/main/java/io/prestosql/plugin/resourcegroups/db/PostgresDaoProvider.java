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
package io.prestosql.plugin.resourcegroups.db;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.postgresql.ds.PGSimpleDataSource;

import javax.inject.Inject;
import javax.inject.Provider;

import static java.util.Objects.requireNonNull;

public class PostgresDaoProvider
        implements Provider<ResourceGroupsDao>
{
    private final PostGresResourceGroupDao dao;

    @Inject
    public PostgresDaoProvider(DbResourceGroupConfig config)
    {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setURL(requireNonNull(config.getConfigDbUrl(), "resource-groups.config-db-url is null"));
        requireNonNull(config, "DbResourceGroupConfig is null");
        this.dao = Jdbi.create(ds)
                .installPlugin(new SqlObjectPlugin())
                .onDemand(PostGresResourceGroupDao.class);
    }

    @Override
    public PostGresResourceGroupDao get()
    {
        return dao;
    }
}
