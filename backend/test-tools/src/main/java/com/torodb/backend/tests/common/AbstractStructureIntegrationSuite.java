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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.torodb.backend.DataTypeProvider;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.backend.ddl.DefaultReadStructure;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.exceptions.InvalidDatabaseException;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractStructureIntegrationSuite {

  private static final String DATABASE_NAME = "database_name";
  private static final String DATABASE_SCHEMA_NAME = "schema_name";
  private static final String COLLECTION_NAME = "collection_name";
  private static final String ROOT_TABLE_NAME = "root_table";
  private static final String CHILD_TABLE_NAME = "child_table";
  private static final String SECOND_CHILD_TABLE_NAME = "second_child_table";
  private static final String FIELD_NAME = "field_name";
  private static final String NEW_COLUMN_NAME = "new_column";

  private SqlInterface sqlInterface;
  private DefaultReadStructure defaultReadStructure;
  private TableRefFactory tableRefFactory;

  private DatabaseTestContext dbTestContext;

  @Before
  public void setUp() throws Exception {
    dbTestContext = getDatabaseTestContext();
    sqlInterface = dbTestContext.getSqlInterface();
    tableRefFactory = new TableRefFactoryImpl();
    DataTypeProvider dataTypeProvider = getDataTypeProvider();
    ErrorHandler errorHandler = getErrorHandler();
    SqlHelper sqlHelper = new SqlHelper(dataTypeProvider, errorHandler);
    SchemaUpdater schemaUpdater = getSchemaUpdater(sqlInterface, sqlHelper);
    defaultReadStructure = getDefaultReadStructure(
        sqlInterface, sqlHelper, schemaUpdater, tableRefFactory);
    dbTestContext.setupDatabase();
  }

  @After
  public void tearDown() throws Exception {
    dbTestContext.tearDownDatabase();
  }

  protected abstract DatabaseTestContext getDatabaseTestContext();

  protected abstract DataTypeProvider getDataTypeProvider();

  protected abstract ErrorHandler getErrorHandler();

  protected abstract SchemaUpdater getSchemaUpdater(
      SqlInterface sqlInterface, SqlHelper sqlHelper);

  protected abstract DefaultReadStructure getDefaultReadStructure(
      SqlInterface sqlInterface, SqlHelper sqlHelper,
      SchemaUpdater schemaUpdater, TableRefFactory tableRefFactory);

  protected abstract String getSqlTypeOf(FieldType fieldType);

  protected char getQuoteChar() {
    return '"';
  }

  @Test
  public void shouldCreateSchema() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      /* When */
      createSchema(dslContext);

      /* Then */
      Connection connection = dslContext.configuration().connectionProvider().acquire();
      try (ResultSet resultSet = getSchemas(connection)) {
        while (resultSet.next()) {
          if (DATABASE_SCHEMA_NAME.equals(resultSet.getString(getSchemaColumn()))) {
            return;
          }
        }

        fail("Schema " + DATABASE_SCHEMA_NAME + " not found");
      } catch (SQLException e) {
        throw new RuntimeException("Wrong test invocation", e);
      }
    });
  }

  protected String getSchemaColumn() {
    return "TABLE_SCHEM";
  }

  protected ResultSet getSchemas(Connection connection) throws SQLException {
    return connection.getMetaData().getSchemas("%", DATABASE_SCHEMA_NAME);
  }

  @Test
  public void shouldCreateRootDocPartTable() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      createSchema(dslContext);

      /* When */
      createRootTable(dslContext, ROOT_TABLE_NAME);

      /* Then */
      Connection connection = dslContext.configuration().connectionProvider().acquire();
      try (Statement foo = connection.createStatement()) {
        ResultSet result = foo.executeQuery(
            "select * from " + getQuoteChar() + DATABASE_SCHEMA_NAME + getQuoteChar()
            + "." + getQuoteChar() + ROOT_TABLE_NAME + getQuoteChar());

        assertThatColumnExists(result.getMetaData(), "did");

        result.close();
      } catch (SQLException e) {
        throw new RuntimeException("Wrong test invocation", e);
      }
    });
  }

  @Test
  public void shouldCreateDocPartTable() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      createSchema(dslContext);
      TableRef rootTableRef = createRootTable(dslContext, ROOT_TABLE_NAME);

      /* When */
      createChildTable(dslContext, rootTableRef, ROOT_TABLE_NAME, CHILD_TABLE_NAME);

      /* Then */
      Connection connection = dslContext.configuration().connectionProvider().acquire();
      try (Statement foo = connection.createStatement()) {
        ResultSet result = foo.executeQuery(
            "select * from " + getQuoteChar() + DATABASE_SCHEMA_NAME + getQuoteChar()
            + "." + getQuoteChar() + CHILD_TABLE_NAME + getQuoteChar());

        assertThatColumnExists(result.getMetaData(), "did");
        assertThatColumnExists(result.getMetaData(), "rid");
        assertThatColumnExists(result.getMetaData(), "seq");

        result.close();
      } catch (SQLException e) {
        throw new RuntimeException("Wrong test invocation", e);
      }
    });
  }

  @Test
  public void shouldCreateSecondLevelDocPartTableWithPid() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      /*Given */
      createSchema(dslContext);
      TableRef rootTableRef = createRootTable(dslContext, ROOT_TABLE_NAME);
      TableRef childTableRef =
          createChildTable(dslContext, rootTableRef, ROOT_TABLE_NAME, CHILD_TABLE_NAME);

      /* When */
      createChildTable(dslContext, childTableRef, CHILD_TABLE_NAME, SECOND_CHILD_TABLE_NAME);

      /* Then */
      Connection connection = dslContext.configuration().connectionProvider().acquire();
      try (Statement foo = connection.createStatement()) {
        ResultSet result = foo.executeQuery(
            "select * from " + getQuoteChar() + DATABASE_SCHEMA_NAME + getQuoteChar()
            + "." + getQuoteChar() + SECOND_CHILD_TABLE_NAME + getQuoteChar());

        assertThatColumnExists(result.getMetaData(), "did");
        assertThatColumnExists(result.getMetaData(), "pid");
        assertThatColumnExists(result.getMetaData(), "rid");
        assertThatColumnExists(result.getMetaData(), "seq");

        result.close();
      } catch (SQLException e) {
        throw new RuntimeException("Wrong test invocation", e);
      }
    });
  }

  @Test
  public void shouldAddColumnsToExistingDocPartTable() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      createSchema(dslContext);
      createRootTable(dslContext, ROOT_TABLE_NAME);

      /* When */
      DataTypeForKv<?> dataType = sqlInterface.getDataTypeProvider().getDataType(FieldType.STRING);
      sqlInterface.getStructureInterface()
          .addColumnToDocPartTable(dslContext, DATABASE_SCHEMA_NAME,
              ROOT_TABLE_NAME, NEW_COLUMN_NAME, dataType);

      /* Then */
      Connection connection = dslContext.configuration().connectionProvider().acquire();
      try (Statement foo = connection.createStatement()) {
        ResultSet result = foo.executeQuery(
            "select * from " + getQuoteChar() + DATABASE_SCHEMA_NAME + getQuoteChar()
            + "." + getQuoteChar() + ROOT_TABLE_NAME + getQuoteChar());

        assertThatColumnIsGivenType(FieldType.STRING, result.getMetaData(), NEW_COLUMN_NAME,
            getSqlTypeOf(FieldType.STRING));

        result.close();
      } catch (SQLException e) {
        throw new RuntimeException("Wrong test invocation", e);
      }
    });
  }

  @Test
  public void newColumnShouldSupportAnyGivenSupportedFieldType() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      createSchema(dslContext);
      createRootTable(dslContext, ROOT_TABLE_NAME);

      Connection connection = dslContext.configuration().connectionProvider().acquire();

      for (FieldType fieldType : FieldType.values()) {
        DataTypeForKv<?> dataType = sqlInterface.getDataTypeProvider().getDataType(fieldType);
        String columnName = NEW_COLUMN_NAME + "_" + fieldType.name();

        sqlInterface.getStructureInterface()
            .addColumnToDocPartTable(dslContext, DATABASE_SCHEMA_NAME,
                ROOT_TABLE_NAME, columnName, dataType);

        try (Statement foo = connection.createStatement()) {
          ResultSet result = foo.executeQuery(
              "select * from " + getQuoteChar() + DATABASE_SCHEMA_NAME + getQuoteChar()
              + "." + getQuoteChar() + ROOT_TABLE_NAME + getQuoteChar());

          assertThatColumnIsGivenType(fieldType, result.getMetaData(),
              columnName, getSqlTypeOf(fieldType));

          result.close();
        } catch (SQLException e) {
          throw new RuntimeException("Wrong test invocation", e);
        }
      }
    });
  }

  @Test
  public void validationSupportAnyGivenSupportedFieldType() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      MetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_NAME, DATABASE_SCHEMA_NAME).build();
      sqlInterface.getMetaDataWriteInterface().addMetaDatabase(dslContext, metaDatabase);

      MetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, ROOT_TABLE_NAME).build();
      sqlInterface.getMetaDataWriteInterface()
          .addMetaCollection(dslContext, metaDatabase, metaCollection);

      TableRef rootTableRef = tableRefFactory.createRoot();
      MetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(rootTableRef, ROOT_TABLE_NAME).build();
      sqlInterface.getMetaDataWriteInterface()
        .addMetaDocPart(dslContext, metaDatabase, metaCollection, metaDocPart);

      createSchema(dslContext);
      createRootTable(dslContext, ROOT_TABLE_NAME);

      for (FieldType fieldType : FieldType.values()) {
        DataTypeForKv<?> dataType = sqlInterface.getDataTypeProvider().getDataType(fieldType);
        String columnName = NEW_COLUMN_NAME + "_" + fieldType.name();

        MetaField metaField = new ImmutableMetaField(FIELD_NAME, columnName, fieldType);
        sqlInterface.getMetaDataWriteInterface()
          .addMetaField(dslContext, metaDatabase, metaCollection, metaDocPart, metaField);

        sqlInterface.getStructureInterface()
            .addColumnToDocPartTable(dslContext, DATABASE_SCHEMA_NAME,
                ROOT_TABLE_NAME, columnName, dataType);
      }

      try {
        defaultReadStructure.readMetadata(dslContext);
      } catch (InvalidDatabaseException ex) {
        fail(ex.getMessage());
      }
    });
  }

  @Test
  public void databaseShouldBeDeleted() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      String collection = ROOT_TABLE_NAME;

      createSchema(dslContext);
      createRootTable(dslContext, collection);

      ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(tableRefFactory.createRoot(),ROOT_TABLE_NAME).build();
      ImmutableMetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(collection, collection).put(metaDocPart).build();
      MetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_SCHEMA_NAME, DATABASE_SCHEMA_NAME)
          .put(metaCollection).build();

      /* When */
      sqlInterface.getStructureInterface().dropDatabase(dslContext, metaDatabase);

      /* Then */
      Connection connection = dslContext.configuration().connectionProvider().acquire();
      try (ResultSet resultSet = getSchemas(connection)) {
        while (resultSet.next()) {
          if (DATABASE_SCHEMA_NAME.equals(resultSet.getString(getSchemaColumn()))) {
            fail("Schema " + DATABASE_SCHEMA_NAME + " shouldn't exist");
          }
        }
      } catch (SQLException e) {
        throw new RuntimeException("Wrong test invocation", e);
      }
    });
  }

  private void createSchema(DSLContext dslContext) {
    sqlInterface.getStructureInterface().createDatabase(dslContext, DATABASE_SCHEMA_NAME);
  }

  private TableRef createRootTable(DSLContext dslContext, String rootName) {
    TableRef rootTableRef = tableRefFactory.createRoot();
    sqlInterface.getStructureInterface().createRootDocPartTable(dslContext,
        DATABASE_SCHEMA_NAME, rootName, rootTableRef);

    return rootTableRef;
  }

  private TableRef createChildTable(DSLContext dslContext, TableRef tableRef, String parentName,
                                    String childName) {

    TableRef childTableRef = tableRefFactory.createChild(tableRef, childName);
    sqlInterface.getStructureInterface().createDocPartTable(dslContext, DATABASE_SCHEMA_NAME,
        childName, childTableRef, parentName);

    return childTableRef;
  }

  private void assertThatColumnExists(ResultSetMetaData metaData, String columnName)
      throws SQLException {

    boolean findMatch = false;

    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      if (columnName.equals(metaData.getColumnLabel(i))) {
        findMatch = true;
      }
    }

    if (!findMatch) {
      assertTrue("Column " + columnName + " should exist", false);
    }
  }

  private void assertThatColumnIsGivenType(FieldType fieldType,
      ResultSetMetaData metaData, String columnName, String requiredType) throws SQLException {

    boolean findMatch = false;

    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      if (columnName.equals(metaData.getColumnLabel(i))) {
        findMatch = true;
        assertEquals("Wrong type for field type " + fieldType,
            requiredType, metaData.getColumnTypeName(i));
      }
    }

    if (!findMatch) {
      assertTrue("Column " + columnName + " should exist", false);
    }
  }

}
