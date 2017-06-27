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
package com.torodb.backend.mysql;

import com.google.inject.PrivateModule;
import com.torodb.backend.BackendConfig;
import com.torodb.backend.BackendConfigImplBuilder;
import com.torodb.backend.mysql.guice.MySqlBackendModule;
import com.torodb.backend.tests.common.BackendTestContext;
import com.torodb.backend.tests.common.BackendTestContextFactory;
import com.torodb.backend.tests.common.IntegrationTestBundleConfig;
import com.torodb.testing.docker.mysql.MysqlService;

public class MySqlTestContextFactory extends BackendTestContextFactory {

  private final MysqlService mysqlService;
  
  public MySqlTestContextFactory(MysqlService mysqlDockerService) {
    this.mysqlService = mysqlDockerService;
  }
  
  @Override
  public BackendTestContext<?> get() {
    BackendConfig config = new BackendConfigImplBuilder(new IntegrationTestBundleConfig())
        .setUsername(mysqlService.getConfig().getUsername())
        .setPassword(mysqlService.getConfig().getPassword())
        .setDbName(mysqlService.getConfig().getDb())
        .setDbHost(mysqlService.getAddress().getHost())
        .setDbPort(mysqlService.getAddress().getPort())
        .setIncludeForeignKeys(false)
        .build();
    return new MySqlTestContext(new MySqlBackendTestBundle(config) {
        @Override
        public PrivateModule getBackendModule(BackendConfig config) {
          return new MySqlBackendTestModule(config);
        }
    });
  }

  private class MySqlBackendTestModule extends MySqlBackendModule {
    public MySqlBackendTestModule(BackendConfig config) {
      super(config);
    }

    @Override
    protected void configure() {
      super.configure();
      
      exposeTestInstances(clazz -> expose(clazz));
    }
  }

}
