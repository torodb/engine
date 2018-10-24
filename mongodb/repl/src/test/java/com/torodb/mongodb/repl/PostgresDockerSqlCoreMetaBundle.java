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

package com.torodb.mongodb.repl;

import com.google.common.net.HostAndPort;
import com.torodb.backend.BackendConfig;
import com.torodb.backend.BackendConfigImplBuilder;
import com.torodb.backend.postgresql.PostgreSqlBackendBundle;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.testing.docker.postgres.PostgresConfig;
import com.torodb.testing.docker.postgres.PostgresService;

/**
 *
 */
public class PostgresDockerSqlCoreMetaBundle extends SqlCoreMetaBundle {

  public PostgresDockerSqlCoreMetaBundle(BackendBundle backendBundle) {
    super(backendBundle);
  }

  public PostgresDockerSqlCoreMetaBundle(BundleConfig generalConfig, BackendBundle backendBundle) {
    super(generalConfig, backendBundle);
  }

  public static PostgresDockerSqlCoreMetaBundle fromDocker(PostgresService dockerService) {
    PostgresConfig postgresConfig = dockerService.getConfig();
    HostAndPort postgresAddress = dockerService.getAddress();

    BackendConfig backendConfig = new BackendConfigImplBuilder(new TestBundleConfig())
        .setDbName(postgresConfig.getDb())
        .setUsername(postgresConfig.getUsername())
        .setPassword(postgresConfig.getPassword())
        .setSslEnabled(false)
        .setDbHost(postgresAddress.getHost())
        .setDbPort(postgresAddress.getPort())
        .setConnectionPoolSize(10)
        .setReservedReadPoolSize(5)
        .build();
    BackendBundle backendBundle = new PostgreSqlBackendBundle(backendConfig);

    return new PostgresDockerSqlCoreMetaBundle(backendBundle);

  }

}
