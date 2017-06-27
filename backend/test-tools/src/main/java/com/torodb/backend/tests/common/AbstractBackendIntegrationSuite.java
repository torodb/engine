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
package com.torodb.backend.tests.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.torodb.core.TableRef;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.kvdocument.values.KvDocument;
import org.jooq.DSLContext;
import org.jooq.lambda.tuple.Tuple3;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@RunWith(JUnitPlatform.class)
public abstract class AbstractBackendIntegrationSuite {

  protected static final String DATABASE_NAME = "database_name";
  protected static final String DATABASE_SCHEMA_NAME = "schema_name";
  protected static final String COLLECTION_NAME = "collection_name";
  protected static final String COLLECTION_IDENTIFIER = "collection_identifier";
  protected static final String ROOT_TABLE_NAME = "root_table";
  protected static final String CHILD_TABLE_NAME = "child_table";
  protected static final String SECOND_CHILD_TABLE_NAME = "second_child_table";
  protected static final String FIELD_NAME = "field_name";
  protected static final String FIELD_COLUMN_NAME = "field_column";
  protected static final String SCALAR_COLUMN_NAME = "scalar_column";
  protected static final String INDEX_NAME = "index_name";
  protected static final String ROOT_INDEX_NAME = "root_index";
  protected static final String INFO_KEY_NAME = "info_key_name";
  protected static final String INFO_KEY_VALUE = "info_key_value";

  protected BackendTestContext<?> context;

  @BeforeEach
  public void setUp() throws Exception {
    context = getBackendTestContextFactory().get();
    context.setupDatabase();
  }

  protected abstract BackendTestContextFactory getBackendTestContextFactory();

  @AfterEach
  public void tearDown() throws Exception {
    context.tearDownDatabase();
  }
  
  public BackendTestContext<?> getContext() {
    return context;
  }

  protected void createSchema(DSLContext dslContext) {
    createSchema(dslContext, DATABASE_SCHEMA_NAME);
  }

  protected void createSchema(DSLContext dslContext, String schemaName) {
    context.getSqlInterface().getStructureInterface()
      .createDatabase(dslContext, schemaName);
  }

  protected TableRef createRootTable(DSLContext dslContext, String rootName) {
    TableRef rootTableRef = context.getTableRefFactory().createRoot();
    context.getSqlInterface().getStructureInterface().createRootDocPartTable(dslContext,
        DATABASE_SCHEMA_NAME, rootName, rootTableRef);

    return rootTableRef;
  }

  protected TableRef createChildTable(DSLContext dslContext, TableRef tableRef, String parentName,
                                    String childName) {
    TableRef childTableRef = context.getTableRefFactory().createChild(tableRef, childName);
    context.getSqlInterface().getStructureInterface()
        .createDocPartTable(dslContext, DATABASE_SCHEMA_NAME,
            childName, childTableRef, parentName);

    return childTableRef;
  }

  protected void assertThatSchemaExists(DSLContext dslContext, 
      String schemaName) throws SQLException {
    execute(dslContext, connection -> {
      ResultSet result = context.getSchemas(connection);
      
      boolean schemaFound = false;
      
      while (result.next()) {
        if (result.getString(context.getSchemaColumn()).equals(schemaName)) {
          schemaFound = true;
          break;
        }
      }
      
      assertTrue("Schema " + schemaName + " should exist", schemaFound);
    });
  }

  protected void assertThatSchemaDoesNotExists(DSLContext dslContext, 
      String schemaName) throws SQLException {
    execute(dslContext, connection -> {
      ResultSet result = context.getSchemas(connection);
      
      boolean schemaFound = false;
      
      while (result.next()) {
        if (result.getString(context.getSchemaColumn()).equals(schemaName)) {
          schemaFound = true;
          break;
        }
      }
      
      assertFalse("Schema " + schemaName + " should not exist", schemaFound);
    });
  }

  protected void assertThatTableExists(DSLContext dslContext, 
      String schemaName, String tableName) throws SQLException {
    execute(dslContext, connection -> {
      ResultSet result = context.getTable(connection, schemaName, tableName);
      
      assertTrue("Table " + tableName + " should exist", result.next());
    });
  }

  protected void assertThatTableDoesNotExists(DSLContext dslContext, 
      String schemaName, String tableName) throws SQLException {
    execute(dslContext, connection -> {
      ResultSet result = context.getTable(connection, schemaName, tableName);
      
      assertFalse("Table " + tableName + " should not exist", result.next());
    });
  }

  protected void assertThatColumnExists(DSLContext dslContext, 
      String schemaName, String tableName, String columnName)
      throws SQLException {
    execute(dslContext, connection -> {
      ResultSet result = context.getColumns(connection, schemaName, tableName);
      
      boolean columnFound = false;
      
      while (result.next()) {
        if (result.getString("COLUMN_NAME").equals(columnName)) {
          columnFound = true;
          break;
        }
      }
      
      assertTrue("Column " + columnName + " should exist", columnFound);
    });
  }

  protected void assertThatColumnIsGivenType(DSLContext dslContext,
      String schemaName, String tableName, String columnName, 
      FieldType fieldType) throws SQLException {
    execute(dslContext, connection -> {
      ResultSet result = context.getColumns(connection, schemaName, tableName);
      
      boolean columnFound = false;
      
      while (result.next()) {
        if (result.getString("COLUMN_NAME").equals(columnName)) {
          assertEquals("Wrong type for field type " + fieldType,
              getSqlTypeOf(fieldType), result.getString("TYPE_NAME").replace("\"", ""));
          columnFound = true;
          break;
        }
      }
      
      assertTrue("Column " + columnName + " should exist", columnFound);
    });
  }

  protected final String getSqlTypeOf(FieldType fieldType) {
    return context.getSqlInterface().getDataTypeProvider().getDataType(fieldType).getTypeName();
  }

  protected void assertThatUniqueIndexExists(DSLContext dslContext, 
      String schemaName, String tableName, String indexName, 
      List<Tuple3<String,Boolean,FieldType>> columns) throws SQLException {
    assertThatIndexExists(dslContext, schemaName, tableName, indexName, columns, true);
  }

  protected void assertThatIndexExists(DSLContext dslContext, 
      String schemaName, String tableName, String indexName, 
      List<Tuple3<String,Boolean,FieldType>> columns) throws SQLException {
    assertThatIndexExists(dslContext, schemaName, tableName, indexName, columns, false);
  }

  protected void assertThatIndexExists(DSLContext dslContext, 
      String schemaName, String tableName, String indexName, 
      List<Tuple3<String, Boolean, FieldType>> columns, boolean unique)
      throws SQLException {
    execute(dslContext, connection -> {
      ResultSet result = context.getIndexes(connection, schemaName, tableName);
      int columnCount = 0;
      boolean found = false;
      String lastIndexName = null;
      Boolean lastNonUnique = null;
      while (result.next()) {
        String foundIndexName = result.getString("INDEX_NAME");
        Boolean nonUnique = result.getBoolean("NON_UNIQUE");
        int position = result.getInt("ORDINAL_POSITION");
        String columnName = result.getString("COLUMN_NAME");
        String ascOrDesc = result.getString("ASC_OR_DESC");
        if (position > 0 
            && columnName.equals(columns.get(position - 1).v1)
            && (ascOrDesc == null || ascOrDesc.equals("A")) 
                == columns.get(position - 1).v2) {
          found = true;
        }
        columnCount++;
        if (lastIndexName != null && !lastIndexName.equals(foundIndexName)) {
          found = found && foundIndexName.equals(indexName)
              && columnCount != columns.size() && unique && !lastNonUnique;
          if (found) {
            break;
          }
          lastIndexName = foundIndexName;
          lastNonUnique = nonUnique;
        }
      }
  
      assertTrue("No matching index found", found);
    });
  }

  protected void assertThatIndexDoesNotExists(DSLContext dslContext, 
      String schemaName, String indexName, String tableName)
      throws SQLException {
    execute(dslContext, connection -> {
      ResultSet result = context.getIndexes(connection, schemaName, tableName);
      assertFalse("Index should not exists", result.next());
    });
  }

  protected void assertThatDataExists(DSLContext dslContext,
      MetaDatabase metaDatabase, MetaCollection metaCollection,
      KvDocument expectedDoc) throws SQLException {
    Cursor<Integer> cursor = context.getSqlInterface().getReadInterface()
        .getAllCollectionDids(dslContext, metaDatabase, metaCollection);
    
    boolean dataFound = false;
    
    while (cursor.hasNext()) {
      int did = cursor.next();
      List<DocPartResult> docPartResult = 
          context.getSqlInterface().getReadInterface().getCollectionResultSets(
              dslContext, metaDatabase, metaCollection, Arrays.asList(did));
      List<ToroDocument> toroDocuments = context.getR2DTranslator().translate(
          docPartResult.iterator());
      assertFalse("Document with did " + did + " should exists", toroDocuments.isEmpty());
      assertEquals("Found more than 1 documents with did " + did, 1, toroDocuments.size());
      if (toroDocuments.get(0).getRoot().equals(expectedDoc)) {
        dataFound = true;
        break;
      }
    }
    
    assertTrue("Data should exist", dataFound);
  }

  protected void assertThatDataDoesNotExists(DSLContext dslContext,
      MetaDatabase metaDatabase, MetaCollection metaCollection) throws SQLException {

    Cursor<Integer> cursor = context.getSqlInterface().getReadInterface()
        .getAllCollectionDids(dslContext, metaDatabase, metaCollection);
    
    assertFalse("Data should not exist", cursor.hasNext());
  }

  protected void commit(DSLContext dslContext) throws SQLException {
    execute(dslContext, connection -> {
      connection.commit();
    });
  }
  
  protected void execute(DSLContext dslContext, Executor executor) throws SQLException {
    Connection connection = dslContext.configuration().connectionProvider().acquire();
    try {
      executor.execute(connection);
    } finally {
      dslContext.configuration().connectionProvider().release(connection);
    }
  }
  
  private interface Executor {
    public void execute(Connection connection) throws SQLException;
  }
}
