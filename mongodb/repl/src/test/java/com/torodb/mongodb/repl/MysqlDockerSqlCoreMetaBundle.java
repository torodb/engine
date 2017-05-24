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
import com.torodb.backend.mysql.MySqlBackendBundle;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.testing.docker.mysql.MysqlConfig;
import com.torodb.testing.docker.mysql.MysqlService;

/**
 *
 */
public class MysqlDockerSqlCoreMetaBundle extends SqlCoreMetaBundle {

  public MysqlDockerSqlCoreMetaBundle(BackendBundle backendBundle) {
    super(backendBundle);
  }

  public MysqlDockerSqlCoreMetaBundle(BundleConfig generalConfig, BackendBundle backendBundle) {
    super(generalConfig, backendBundle);
  }

  public static MysqlDockerSqlCoreMetaBundle fromDocker(MysqlService dockerService) {
    MysqlConfig mysqlConfig = dockerService.getConfig();
    HostAndPort mysqlAddress = dockerService.getAddress();

    BackendConfig backendConfig = new BackendConfigImplBuilder(new TestBundleConfig())
        .setDbName(mysqlConfig.getDb())
        .setUsername(mysqlConfig.getUsername())
        .setPassword(mysqlConfig.getPassword())
        .setSslEnabled(false)
        .setDbHost(mysqlAddress.getHost())
        .setDbPort(mysqlAddress.getPort())
        .setConnectionPoolSize(10)
        .setReservedReadPoolSize(5)
        .build();
    BackendBundle backendBundle = new MySqlBackendBundle(backendConfig);

    return new MysqlDockerSqlCoreMetaBundle(backendBundle);

  }

}
