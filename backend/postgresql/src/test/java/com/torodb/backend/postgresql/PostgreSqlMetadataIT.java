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


import com.torodb.backend.tests.common.AbstractMetaDataIntegrationSuite;
import com.torodb.backend.tests.common.DatabaseTestContext;
import com.torodb.testing.docker.postgres.EnumVersion;
import com.torodb.testing.docker.postgres.PostgresService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class PostgreSqlMetadataIT extends AbstractMetaDataIntegrationSuite {

  private static PostgresService postgresDockerService;

  @BeforeClass
  public static void beforeAll() {
    postgresDockerService = PostgresService.defaultService(EnumVersion.LATEST);
    postgresDockerService.startAsync();
    postgresDockerService.awaitRunning();
  }

  @AfterClass
  public static void afterAll() {
    if (postgresDockerService != null && postgresDockerService.isRunning()) {
      postgresDockerService.stopAsync();
      postgresDockerService.awaitTerminated();
    }
  }

  @Override
  protected DatabaseTestContext getDatabaseTestContext() {
    return new PostgreSqlDatabaseTestContextFactory().createInstance(postgresDockerService);
  }

}
