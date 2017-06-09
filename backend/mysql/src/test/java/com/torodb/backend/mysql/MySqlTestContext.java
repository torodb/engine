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

import com.torodb.backend.tests.common.BackendTestContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

public class MySqlTestContext extends BackendTestContext<MySqlBackendTestBundle> {

  public MySqlTestContext(MySqlBackendTestBundle bundle) {
    super(bundle);
  }
  
  @Override
  protected String getDropSchemaStatement(String schemaName) {
    return "DROP DATABASE " + getQuoteChar() + schemaName + getQuoteChar();
  }

  @Override
  protected char getQuoteChar() {
    return '`';
  }

  @Override
  protected ResultSet getSchemas(Connection connection) throws SQLException {
    return connection.getMetaData().getCatalogs();
  }

  @Override
  protected String getSchemaColumn() {
    return "TABLE_CAT";
  }

  @Override
  protected ResultSet getTables(Connection connection, String schemaName)
      throws SQLException {
    return connection.getMetaData().getTables(schemaName, "%", "%", null);
  }

  @Override
  protected ResultSet getTable(Connection connection, String schemaName, String tableName)
      throws SQLException {
    return connection.getMetaData().getTables(schemaName, "%", tableName, null);
  }

  @Override
  protected ResultSet getColumns(Connection connection, String schemaName, String tableName)
      throws SQLException {
    return connection.getMetaData().getColumns(schemaName, "%", tableName, "%");
  }

  @Override
  protected ResultSet getIndexes(Connection connection, String schemaName, String tableName)
      throws SQLException {
    return connection.getMetaData().getIndexInfo(schemaName, "%", tableName, false, false);
  }

  @Override
  protected Function<SQLException, Boolean> getOnDropTableRetryStrategy() {
    return ex -> !ex.getSQLState().equals("42S02");
  }

}

