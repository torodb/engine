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

import com.torodb.backend.tests.common.AbstractStructureIntegrationSuite;
import com.torodb.backend.tests.common.BackendTestContextFactory;
import com.torodb.testing.docker.postgres.EnumVersion;
import com.torodb.testing.docker.postgres.PostgresService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class PostgreSqlStructureIT extends AbstractStructureIntegrationSuite {

  private static PostgresService postgresService;

  @BeforeAll
  public static void beforeAll() {
    postgresService = PostgresService.defaultService(EnumVersion.LATEST);
    postgresService.startAsync();
    postgresService.awaitRunning();
  }

  @AfterAll
  public static void afterAll() {
    if (postgresService != null && postgresService.isRunning()) {
      postgresService.stopAsync();
      postgresService.awaitTerminated();
    }
  }

  @Override
  protected BackendTestContextFactory getBackendTestContextFactory() {
    return new PostgreSqlTestContextFactory(postgresService);
  }

}
