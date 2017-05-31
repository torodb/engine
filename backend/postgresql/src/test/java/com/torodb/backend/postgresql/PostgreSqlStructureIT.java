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

import com.torodb.backend.DataTypeProvider;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.ddl.DefaultReadStructure;
import com.torodb.backend.tests.common.AbstractStructureIntegrationSuite;
import com.torodb.backend.tests.common.DatabaseTestContext;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.testing.docker.postgres.EnumVersion;
import com.torodb.testing.docker.postgres.PostgresService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.HashMap;
import java.util.Map;

public class PostgreSqlStructureIT extends AbstractStructureIntegrationSuite {

  private static PostgresService postgresDockerService;

  private Map<FieldType, String> typesDictionary = new HashMap<>();

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

  public PostgreSqlStructureIT() {
    typesDictionary.put(FieldType.STRING, "varchar");
    typesDictionary.put(FieldType.BINARY, "bytea");
    typesDictionary.put(FieldType.BOOLEAN, "bool");
    typesDictionary.put(FieldType.DATE, "date");
    typesDictionary.put(FieldType.DOUBLE, "float8");
    typesDictionary.put(FieldType.INSTANT, "timestamptz");
    typesDictionary.put(FieldType.INTEGER, "int4");
    typesDictionary.put(FieldType.LONG, "int8");
    typesDictionary.put(FieldType.MONGO_OBJECT_ID, "bytea");
    typesDictionary.put(FieldType.MONGO_TIME_STAMP, "\"torodb\".\"mongo_timestamp\"");
    typesDictionary.put(FieldType.NULL, "bool");
    typesDictionary.put(FieldType.TIME, "time");
    typesDictionary.put(FieldType.CHILD, "bool");
    typesDictionary.put(FieldType.DECIMAL128, "\"torodb\".\"decimal_128\"");
    typesDictionary.put(FieldType.JAVASCRIPT, "varchar");
    typesDictionary.put(FieldType.JAVASCRIPT_WITH_SCOPE, "jsonb");
    typesDictionary.put(FieldType.MIN_KEY, "bool");
    typesDictionary.put(FieldType.MAX_KEY, "bool");
    typesDictionary.put(FieldType.UNDEFINED, "bool");
    typesDictionary.put(FieldType.MONGO_REGEX, "jsonb");
    typesDictionary.put(FieldType.MONGO_DB_POINTER, "jsonb");
    typesDictionary.put(FieldType.DEPRECATED, "varchar");
  }

  @Override
  protected DatabaseTestContext getDatabaseTestContext() {
    return new PostgreSqlDatabaseTestContextFactory().createInstance(postgresDockerService);
  }

  @Override
  protected DataTypeProvider getDataTypeProvider() {
    return new PostgreSqlDataTypeProvider();
  }

  @Override
  protected ErrorHandler getErrorHandler() {
    return new PostgreSqlErrorHandler();
  }

  @Override
  protected DefaultReadStructure getDefaultReadStructure(SqlInterface sqlInterface, SqlHelper sqlHelper,
      TableRefFactory tableRefFactory) {
    return new DefaultReadStructure(sqlInterface, sqlHelper, tableRefFactory);
  }

  @Override
  protected String getSqlTypeOf(FieldType fieldType) {
    if (!typesDictionary.containsKey(fieldType))
      throw new RuntimeException("Unsupported type " + fieldType.name());

    return typesDictionary.get(fieldType);
  }

}
