/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.backend.postgresql;

import com.google.inject.PrivateModule;
import com.torodb.backend.BackendConfig;
import com.torodb.backend.BackendConfigImplBuilder;
import com.torodb.backend.postgresql.guice.PostgreSqlBackendModule;
import com.torodb.backend.tests.common.BackendTestContext;
import com.torodb.backend.tests.common.BackendTestContextFactory;
import com.torodb.backend.tests.common.IntegrationTestBundleConfig;
import com.torodb.testing.docker.postgres.PostgresService;

public class PostgreSqlTestContextFactory extends BackendTestContextFactory {

  private final PostgresService postgresService;
  
  public PostgreSqlTestContextFactory(PostgresService postgresDockerService) {
    this.postgresService = postgresDockerService;
  }
  
  @Override
  public BackendTestContext<?> get() {
    BackendConfig config = new BackendConfigImplBuilder(new IntegrationTestBundleConfig())
        .setUsername(postgresService.getConfig().getUsername())
        .setPassword(postgresService.getConfig().getPassword())
        .setDbName(postgresService.getConfig().getDb())
        .setDbHost(postgresService.getAddress().getHost())
        .setDbPort(postgresService.getAddress().getPort())
        .setConnectionPoolTimeout(100_000L)
        .setConnectionPoolSize(10)
        .setReservedReadPoolSize(5)
        .setIncludeForeignKeys(false)
        .build();
    return new PostgreSqlTestContext(new PostgreSqlBackendTestBundle(config) {
        @Override
        public PrivateModule getBackendModule(BackendConfig config) {
          return new PostgreSqlBackendTestModule(config);
        }
    });
  }

  private class PostgreSqlBackendTestModule extends PostgreSqlBackendModule {
    public PostgreSqlBackendTestModule(BackendConfig config) {
      super(config);
    }

    @Override
    protected void configure() {
      super.configure();
      
      exposeTestInstances(clazz -> expose(clazz));
    }
  }

}
