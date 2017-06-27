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


import com.torodb.backend.tests.common.AbstractMetaDataIntegrationSuite;
import com.torodb.backend.tests.common.BackendTestContextFactory;
import com.torodb.testing.docker.mysql.EnumVersion;
import com.torodb.testing.docker.mysql.MysqlService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class MySqlMetadataIT extends AbstractMetaDataIntegrationSuite {

  private static MysqlService mysqlService;

  @BeforeAll
  public static void beforeAll() {
    mysqlService = MysqlService.defaultService(EnumVersion.LATEST);
    mysqlService.startAsync();
    mysqlService.awaitRunning();
  }

  @AfterAll
  public static void afterAll() {
    if (mysqlService != null && mysqlService.isRunning()) {
      mysqlService.stopAsync();
      mysqlService.awaitTerminated();
    }
  }

  @Override
  protected BackendTestContextFactory getBackendTestContextFactory() {
    return new MySqlTestContextFactory(mysqlService);
  }

}
