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

import com.torodb.backend.DataTypeProvider;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.ddl.DefaultReadStructure;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.mysql.meta.MySqlSchemaUpdater;
import com.torodb.backend.mysql.meta.MySqlReadStructure;
import com.torodb.backend.tests.common.AbstractStructureIntegrationSuite;
import com.torodb.backend.tests.common.DatabaseTestContext;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MySqlStructureIT extends AbstractStructureIntegrationSuite {

  private Map<FieldType, String> typesDictionary = new HashMap<>();

  public MySqlStructureIT() {
    typesDictionary.put(FieldType.STRING, "TEXT");
    typesDictionary.put(FieldType.BINARY, "BLOB");
    typesDictionary.put(FieldType.BOOLEAN, "BIT");
    typesDictionary.put(FieldType.DATE, "DATE");
    typesDictionary.put(FieldType.DOUBLE, "DOUBLE");
    typesDictionary.put(FieldType.INSTANT, "TIMESTAMP");
    typesDictionary.put(FieldType.INTEGER, "INT");
    typesDictionary.put(FieldType.LONG, "BIGINT");
    typesDictionary.put(FieldType.MONGO_OBJECT_ID, "VARBINARY");
    typesDictionary.put(FieldType.MONGO_TIME_STAMP, "TEXT");
    typesDictionary.put(FieldType.NULL, "BIT");
    typesDictionary.put(FieldType.TIME, "TIME");
    typesDictionary.put(FieldType.CHILD, "BIT");
    typesDictionary.put(FieldType.DECIMAL128, "TEXT");
    typesDictionary.put(FieldType.JAVASCRIPT, "TEXT");
    typesDictionary.put(FieldType.JAVASCRIPT_WITH_SCOPE, "TEXT");
    typesDictionary.put(FieldType.MIN_KEY, "BIT");
    typesDictionary.put(FieldType.MAX_KEY, "BIT");
    typesDictionary.put(FieldType.UNDEFINED, "BIT");
    typesDictionary.put(FieldType.MONGO_REGEX, "TEXT");
    typesDictionary.put(FieldType.MONGO_DB_POINTER, "TEXT");
    typesDictionary.put(FieldType.DEPRECATED, "TEXT");
  }
  
  @Override
  protected char getQuoteChar() {
    return '`';
  }

  @Override
  protected DatabaseTestContext getDatabaseTestContext() {
    return new MySqlDatabaseTestContextFactory().createInstance();
  }

  @Override
  protected DataTypeProvider getDataTypeProvider() {
    return new MySqlDataTypeProvider();
  }

  @Override
  protected ErrorHandler getErrorHandler() {
    return new MySqlErrorHandler();
  }

  @Override
  protected SchemaUpdater getSchemaUpdater(SqlInterface sqlInterface, SqlHelper sqlHelper) {
    return new MySqlSchemaUpdater(sqlInterface, sqlHelper);
  }

  @Override
  protected DefaultReadStructure getDefaultReadStructure(SqlInterface sqlInterface, SqlHelper sqlHelper,
                                                    SchemaUpdater schemaUpdater, TableRefFactory tableRefFactory) {
    return new MySqlReadStructure(sqlInterface, sqlHelper, schemaUpdater, tableRefFactory);
  }

  @Override
  protected String getSqlTypeOf(FieldType fieldType) {
    if (!typesDictionary.containsKey(fieldType))
      throw new RuntimeException("Unsupported type " + fieldType.name());

    return typesDictionary.get(fieldType);
  }

  @Override
  protected String getSchemaColumn() {
    return "TABLE_CAT";
  }

  @Override
  protected ResultSet getSchemas(Connection connection) throws SQLException {
    return connection.getMetaData().getCatalogs();
  }

}
