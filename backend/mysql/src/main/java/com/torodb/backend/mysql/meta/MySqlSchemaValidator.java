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
package com.torodb.backend.mysql.meta;

import com.google.common.collect.ImmutableList;
import com.torodb.backend.exceptions.InvalidDatabaseSchemaException;
import com.torodb.backend.meta.SchemaValidator;
import com.torodb.core.exceptions.SystemException;
import org.jooq.DSLContext;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySqlSchemaValidator extends SchemaValidator {

  public MySqlSchemaValidator(DSLContext dsl, String schemaName, String database)
      throws InvalidDatabaseSchemaException {
    super(dsl, schemaName, database);
  }

  @Override
  protected void checkDatabaseSchema(Connection connection) throws InvalidDatabaseSchemaException {
    try {
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet resultSet = metaData.getCatalogs();
      while (resultSet.next()) {
        if (resultSet.getString("TABLE_CAT").equals(schemaName)) {
          return;
        }
      }

      throw new IllegalStateException(
          "The database " + database + " is associated with schema "
          + schemaName + " but there is no schema with that name");
    } catch (SQLException sqlException) {
      throw new SystemException(sqlException);
    }
  }

  @Override
  protected SchemaValidator.Table.ResultSetIterator getTableIterator(
      String schemaName, Connection connection) {
    return new Table.ResultSetIterator(schemaName, connection);
  }

  @Override
  protected SchemaValidator.Index.ResultSetIterator getIndexIterator(
      String schemaName, Connection connection, SchemaValidator.Table table) {
    return new Index.ResultSetIterator(schemaName, table.getName(), connection);
  }

  public static class Index extends SchemaValidator.Index {
    
    public Index(String schema, String name, boolean unique, ImmutableList<IndexField> fields) {
      super(schema, name, unique, fields);
    }

    public static class ResultSetIterator extends SchemaValidator.Index.ResultSetIterator {
  
      public ResultSetIterator(String schemaName, String tableName, Connection connection) {
        super("TABLE_CAT", schemaName, tableName, connection);
      }
      
      @Override
      protected ResultSet getIndexInfo(DatabaseMetaData metaData, String schemaName,
          String tableName) throws SQLException {
        return metaData.getIndexInfo(null, schemaName, tableName, false, true);
      }
    }
    
  }
  
  public static class Table extends SchemaValidator.Table {
    
    public Table(String schema, String name, ImmutableList<TableField> fields) {
      super(schema, name, fields);
    }

    public static class ResultSetIterator extends SchemaValidator.Table.ResultSetIterator {
  
      public ResultSetIterator(String schemaName, Connection connection) {
        super(schemaName, connection);
      }

      @Override
      protected ResultSet getTables(DatabaseMetaData metaData, String schemaName) 
          throws SQLException {
        return metaData.getTables(schemaName, null, "%", null);
      }

      @Override
      protected ResultSet getColumns(String catalog, String schema, String name)
          throws SQLException {
        return metaData.getColumns(catalog, schema, name, "%");
      }
    }
    
  }

}
