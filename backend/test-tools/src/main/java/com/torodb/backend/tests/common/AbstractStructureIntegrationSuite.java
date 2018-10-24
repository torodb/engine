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

import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.tables.KvTable;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartIndexColumnTable;
import com.torodb.backend.tables.MetaDocPartIndexTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.backend.tables.MetaIndexFieldTable;
import com.torodb.backend.tables.MetaIndexTable;
import com.torodb.backend.tables.MetaScalarTable;
import com.torodb.core.TableRef;
import com.torodb.core.exceptions.InvalidDatabaseException;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import org.jooq.lambda.tuple.Tuple3;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public abstract class AbstractStructureIntegrationSuite extends AbstractBackendIntegrationSuite {

  @Test
  public void shouldCreateSchema() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /* When */
      createSchema(dslContext);

      /* Then */
      assertThatSchemaExists(dslContext, DATABASE_SCHEMA_NAME);
    });
  }

  @Test
  public void shouldCreateRootDocPartTable() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      createSchema(dslContext);

      /* When */
      createRootTable(dslContext, ROOT_TABLE_NAME);

      /* Then */

      assertThatTableExists(dslContext, DATABASE_SCHEMA_NAME, ROOT_TABLE_NAME);
      
      assertThatColumnExists(dslContext, 
          DATABASE_SCHEMA_NAME, ROOT_TABLE_NAME, "did");
    });
  }

  @Test
  public void shouldCreateDocPartTable() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      createSchema(dslContext);
      TableRef rootTableRef = createRootTable(dslContext, ROOT_TABLE_NAME);

      /* When */
      createChildTable(dslContext, rootTableRef, ROOT_TABLE_NAME, CHILD_TABLE_NAME);

      /* Then */
      assertThatTableExists(dslContext, 
          DATABASE_SCHEMA_NAME, CHILD_TABLE_NAME);

      assertThatColumnExists(dslContext, 
          DATABASE_SCHEMA_NAME, CHILD_TABLE_NAME, "did");
      assertThatColumnExists(dslContext, 
          DATABASE_SCHEMA_NAME, CHILD_TABLE_NAME, "rid");
      assertThatColumnExists(dslContext, 
          DATABASE_SCHEMA_NAME, CHILD_TABLE_NAME, "seq");
    });
  }

  @Test
  public void shouldCreateSecondLevelDocPartTableWithPid() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /*Given */
      createSchema(dslContext);
      TableRef rootTableRef = createRootTable(dslContext, ROOT_TABLE_NAME);
      TableRef childTableRef =
          createChildTable(dslContext, rootTableRef, ROOT_TABLE_NAME, CHILD_TABLE_NAME);

      /* When */
      createChildTable(dslContext, childTableRef, CHILD_TABLE_NAME, SECOND_CHILD_TABLE_NAME);

      /* Then */
      assertThatTableExists(dslContext, DATABASE_SCHEMA_NAME, SECOND_CHILD_TABLE_NAME);
      
      assertThatColumnExists(dslContext, 
          DATABASE_SCHEMA_NAME, SECOND_CHILD_TABLE_NAME, "did");
      assertThatColumnExists(dslContext, 
          DATABASE_SCHEMA_NAME, SECOND_CHILD_TABLE_NAME, "pid");
      assertThatColumnExists(dslContext, 
          DATABASE_SCHEMA_NAME, SECOND_CHILD_TABLE_NAME, "rid");
      assertThatColumnExists(dslContext, 
          DATABASE_SCHEMA_NAME, SECOND_CHILD_TABLE_NAME, "seq");
    });
  }

  @Test
  public void shouldDeleteDatabase() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      createSchema(dslContext);
      createRootTable(dslContext, ROOT_TABLE_NAME);

      ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(context.getTableRefFactory().createRoot(),ROOT_TABLE_NAME).build();
      ImmutableMetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER).put(metaDocPart).build();
      MetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_SCHEMA_NAME, DATABASE_SCHEMA_NAME)
          .put(metaCollection).build();

      /* When */
      context.getSqlInterface().getStructureInterface().dropDatabase(dslContext, metaDatabase);

      /* Then */
      assertThatSchemaDoesNotExists(dslContext, DATABASE_SCHEMA_NAME);
    });
  }

  @Test
  public void shouldDeleteIndex() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      FieldType fieldType = FieldType.INTEGER;
      createSchema(dslContext);
      createRootTable(dslContext, ROOT_TABLE_NAME);
      DataTypeForKv<?> dataType = context.getSqlInterface()
          .getDataTypeProvider().getDataType(fieldType);
      context.getSqlInterface().getStructureInterface()
          .addColumnToDocPartTable(dslContext, DATABASE_SCHEMA_NAME,
              ROOT_TABLE_NAME, FIELD_COLUMN_NAME, dataType);
      context.getSqlInterface().getStructureInterface()
          .createIndex(dslContext, ROOT_INDEX_NAME, DATABASE_SCHEMA_NAME, ROOT_TABLE_NAME, 
              ImmutableList.of(
                new Tuple3<String,Boolean,FieldType>(FIELD_COLUMN_NAME, true, fieldType)
              ), 
              false);

      /* When */
      context.getSqlInterface().getStructureInterface()
          .dropIndex(dslContext, DATABASE_SCHEMA_NAME, ROOT_TABLE_NAME, ROOT_INDEX_NAME);

      /* Then */
      assertThatIndexDoesNotExists(dslContext, DATABASE_SCHEMA_NAME, INDEX_NAME, ROOT_TABLE_NAME);
    });
  }

  @Test
  public void shouldDeleteCollection() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      TableRef rootTableRef = context.getTableRefFactory().createRoot();

      ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(rootTableRef, ROOT_TABLE_NAME).build();
      final MetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER)
          .put(metaDocPart)
          .build();
      
      FieldType fieldType = FieldType.INTEGER;
      createSchema(dslContext);
      createRootTable(dslContext, ROOT_TABLE_NAME);
      DataTypeForKv<?> dataType = context.getSqlInterface()
          .getDataTypeProvider().getDataType(fieldType);
      context.getSqlInterface().getStructureInterface()
          .addColumnToDocPartTable(dslContext, DATABASE_SCHEMA_NAME,
              ROOT_TABLE_NAME, FIELD_COLUMN_NAME, dataType);
      context.getSqlInterface().getStructureInterface()
          .createIndex(dslContext, ROOT_INDEX_NAME, DATABASE_SCHEMA_NAME, ROOT_TABLE_NAME, 
              ImmutableList.of(
                new Tuple3<String,Boolean,FieldType>(FIELD_COLUMN_NAME, true, fieldType)
              ), 
              false);

      /* When */
      context.getSqlInterface().getStructureInterface()
          .dropCollection(dslContext, DATABASE_SCHEMA_NAME, metaCollection);

      /* Then */
      assertThatTableDoesNotExists(dslContext, DATABASE_SCHEMA_NAME, ROOT_TABLE_NAME);
      assertThatIndexDoesNotExists(dslContext, DATABASE_SCHEMA_NAME, INDEX_NAME, ROOT_TABLE_NAME);
    });
  }

  @Test
  public void shouldDeleteAll() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      TableRef rootTableRef = context.getTableRefFactory().createRoot();

      ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(rootTableRef, ROOT_TABLE_NAME).build();
      ImmutableMetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER)
          .put(metaDocPart)
          .build();
      ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_NAME, DATABASE_SCHEMA_NAME)
          .put(metaCollection)
          .build();

      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDatabase(dslContext, metaDatabase);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaCollection(dslContext, metaDatabase, metaCollection);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDocPart(dslContext, metaDatabase, metaCollection, metaDocPart);

      createSchema(dslContext);
      createRootTable(dslContext, ROOT_TABLE_NAME);
      
      /* When */
      context.getSqlInterface().getStructureInterface().dropAll(dslContext);

      /* Then */
      assertThatSchemaDoesNotExists(dslContext, DATABASE_SCHEMA_NAME);
      assertThatSchemaDoesNotExists(dslContext, TorodbSchema.IDENTIFIER);
    });
  }

  @Test
  public void shouldDeleteUserData() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      TableRef rootTableRef = context.getTableRefFactory().createRoot();

      ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(rootTableRef, ROOT_TABLE_NAME).build();
      ImmutableMetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER)
          .put(metaDocPart)
          .build();
      ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_NAME, DATABASE_SCHEMA_NAME)
          .put(metaCollection)
          .build();

      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDatabase(dslContext, metaDatabase);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaCollection(dslContext, metaDatabase, metaCollection);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDocPart(dslContext, metaDatabase, metaCollection, metaDocPart);

      createSchema(dslContext);
      createRootTable(dslContext, ROOT_TABLE_NAME);
      
      /* When */
      context.getSqlInterface().getStructureInterface().dropUserData(dslContext);

      /* Then */
      assertThatSchemaDoesNotExists(dslContext, DATABASE_SCHEMA_NAME);
      assertThatSchemaExists(dslContext, TorodbSchema.IDENTIFIER);
      assertThatTableExists(dslContext, TorodbSchema.IDENTIFIER, MetaDatabaseTable.TABLE_NAME);
      assertThatTableExists(dslContext, TorodbSchema.IDENTIFIER, MetaCollectionTable.TABLE_NAME);
      assertThatTableExists(dslContext, TorodbSchema.IDENTIFIER, MetaIndexTable.TABLE_NAME);
      assertThatTableExists(dslContext, TorodbSchema.IDENTIFIER, MetaIndexFieldTable.TABLE_NAME);
      assertThatTableExists(dslContext, TorodbSchema.IDENTIFIER, MetaDocPartTable.TABLE_NAME);
      assertThatTableExists(dslContext, TorodbSchema.IDENTIFIER, MetaFieldTable.TABLE_NAME);
      assertThatTableExists(dslContext, TorodbSchema.IDENTIFIER, MetaScalarTable.TABLE_NAME);
      assertThatTableExists(dslContext, TorodbSchema.IDENTIFIER, MetaDocPartIndexTable.TABLE_NAME);
      assertThatTableExists(dslContext, TorodbSchema.IDENTIFIER, 
          MetaDocPartIndexColumnTable.TABLE_NAME);
      assertThatTableExists(dslContext, TorodbSchema.IDENTIFIER, KvTable.TABLE_NAME);
    });
  }

  @Test
  public void shouldMoveCollection() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      TableRef rootTableRef = context.getTableRefFactory().createRoot();

      ImmutableMetaDocPartIndexColumn metaDocPartIndexColumn = 
          new ImmutableMetaDocPartIndexColumn(0, FIELD_COLUMN_NAME, FieldIndexOrdering.ASC);
      ImmutableMetaIdentifiedDocPartIndex metaDocPartIndex = new ImmutableMetaIdentifiedDocPartIndex
          .Builder(ROOT_INDEX_NAME, false)
          .add(metaDocPartIndexColumn)
          .build();
      ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(rootTableRef, ROOT_TABLE_NAME)
          .put(metaDocPartIndex)
          .build();
      final MetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER)
          .put(metaDocPart)
          .build();

      ImmutableMetaIdentifiedDocPartIndex newMetaDocPartIndex = 
          new ImmutableMetaIdentifiedDocPartIndex.Builder("new_" + ROOT_INDEX_NAME, false)
          .add(metaDocPartIndexColumn)
          .build();
      ImmutableMetaDocPart newMetaDocPart = new ImmutableMetaDocPart
          .Builder(rootTableRef, "new_" + ROOT_TABLE_NAME)
          .put(newMetaDocPartIndex)
          .build();
      final MetaCollection newMetaCollection = new ImmutableMetaCollection
          .Builder("new_" + COLLECTION_NAME, "new_" + COLLECTION_IDENTIFIER)
          .put(newMetaDocPart)
          .build();
      
      createSchema(dslContext);
      createSchema(dslContext, "new_" + DATABASE_SCHEMA_NAME);
      createRootTable(dslContext, ROOT_TABLE_NAME);
      FieldType fieldType = FieldType.INTEGER;
      DataTypeForKv<?> dataType = context.getSqlInterface()
          .getDataTypeProvider().getDataType(fieldType);
      context.getSqlInterface().getStructureInterface()
          .addColumnToDocPartTable(dslContext, DATABASE_SCHEMA_NAME,
              ROOT_TABLE_NAME, FIELD_COLUMN_NAME, dataType);
      context.getSqlInterface().getStructureInterface()
          .createIndex(dslContext, ROOT_INDEX_NAME, DATABASE_SCHEMA_NAME, ROOT_TABLE_NAME, 
              ImmutableList.of(
                new Tuple3<String,Boolean,FieldType>(FIELD_COLUMN_NAME, true, fieldType)
              ), 
              false);
      
      /* When */
      context.getSqlInterface().getStructureInterface()
          .renameCollection(dslContext, DATABASE_SCHEMA_NAME, metaCollection, 
              "new_" + DATABASE_SCHEMA_NAME, newMetaCollection);

      /* Then */
      assertThatTableExists(dslContext, "new_" + DATABASE_SCHEMA_NAME, "new_" + ROOT_TABLE_NAME);
      assertThatIndexExists(dslContext, 
          "new_" + DATABASE_SCHEMA_NAME, "new_" + ROOT_TABLE_NAME, "new_" + ROOT_INDEX_NAME,
          ImmutableList.of(
              new Tuple3<String,Boolean,FieldType>(FIELD_COLUMN_NAME, true, fieldType)
          ));
    });
  }
  
  @ParameterizedTest
  @EnumSource(FieldType.class)
  public void shouldAddColumns(FieldType fieldType) throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      createSchema(dslContext);
      createRootTable(dslContext, ROOT_TABLE_NAME);

      /* When */
      DataTypeForKv<?> dataType = context.getSqlInterface()
          .getDataTypeProvider().getDataType(fieldType);
      context.getSqlInterface().getStructureInterface()
          .addColumnToDocPartTable(dslContext, DATABASE_SCHEMA_NAME,
              ROOT_TABLE_NAME, FIELD_COLUMN_NAME, dataType);

      /* Then */
      assertThatColumnIsGivenType(dslContext, 
          DATABASE_SCHEMA_NAME, ROOT_TABLE_NAME, FIELD_COLUMN_NAME,
          fieldType);
    });
  }

  @ParameterizedTest
  @EnumSource(FieldType.class)
  public void shouldValidateColumn(FieldType fieldType) throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      MetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_NAME, DATABASE_SCHEMA_NAME).build();
      context.getSqlInterface().getMetaDataWriteInterface()
      .addMetaDatabase(dslContext, metaDatabase);

      MetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, ROOT_TABLE_NAME).build();
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaCollection(dslContext, metaDatabase, metaCollection);

      TableRef rootTableRef = context.getTableRefFactory().createRoot();
      MetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(rootTableRef, ROOT_TABLE_NAME).build();
      context.getSqlInterface().getMetaDataWriteInterface()
        .addMetaDocPart(dslContext, metaDatabase, metaCollection, metaDocPart);

      createSchema(dslContext);
      createRootTable(dslContext, ROOT_TABLE_NAME);

      DataTypeForKv<?> dataType = context.getSqlInterface()
          .getDataTypeProvider().getDataType(fieldType);

      MetaField metaField = new ImmutableMetaField(FIELD_NAME, FIELD_COLUMN_NAME, fieldType);
      context.getSqlInterface().getMetaDataWriteInterface()
        .addMetaField(dslContext, metaDatabase, metaCollection, metaDocPart, metaField);

      context.getSqlInterface().getStructureInterface()
          .addColumnToDocPartTable(dslContext, DATABASE_SCHEMA_NAME,
              ROOT_TABLE_NAME, FIELD_COLUMN_NAME, dataType);

      try {
        context.getDdlOps().getReadStructureDdlOp().readMetadata(dslContext);
      } catch (InvalidDatabaseException ex) {
        fail(ex.getMessage());
      }
    });
  }

  @ParameterizedTest
  @EnumSource(FieldType.class)
  public void shouldCreateIndex(FieldType fieldType) throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      createSchema(dslContext);
      createRootTable(dslContext, ROOT_TABLE_NAME);
      DataTypeForKv<?> dataType = context.getSqlInterface()
          .getDataTypeProvider().getDataType(fieldType);
      context.getSqlInterface().getStructureInterface()
          .addColumnToDocPartTable(dslContext, DATABASE_SCHEMA_NAME,
              ROOT_TABLE_NAME, FIELD_COLUMN_NAME, dataType);

      /* When */
      context.getSqlInterface().getStructureInterface()
          .createIndex(dslContext, ROOT_INDEX_NAME, DATABASE_SCHEMA_NAME, ROOT_TABLE_NAME, 
              ImmutableList.of(
                new Tuple3<String,Boolean,FieldType>(FIELD_COLUMN_NAME, true, fieldType)
              ), 
              false);

      /* Then */
      assertThatIndexExists(dslContext,
          DATABASE_SCHEMA_NAME, ROOT_TABLE_NAME, ROOT_INDEX_NAME,
          ImmutableList.of(
              new Tuple3<String,Boolean,FieldType>(FIELD_COLUMN_NAME, true, fieldType)
            ));
    });
  }

  @ParameterizedTest
  @EnumSource(FieldType.class)
  public void shouldCreateUniqueIndex(FieldType fieldType) throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      createSchema(dslContext);
      createRootTable(dslContext, ROOT_TABLE_NAME);
      DataTypeForKv<?> dataType = context.getSqlInterface()
          .getDataTypeProvider().getDataType(fieldType);
      context.getSqlInterface().getStructureInterface()
          .addColumnToDocPartTable(dslContext, DATABASE_SCHEMA_NAME,
              ROOT_TABLE_NAME, FIELD_COLUMN_NAME, dataType);

      /* When */
      context.getSqlInterface().getStructureInterface()
          .createIndex(dslContext, ROOT_INDEX_NAME, DATABASE_SCHEMA_NAME, ROOT_TABLE_NAME, 
              ImmutableList.of(
                new Tuple3<String,Boolean,FieldType>(FIELD_COLUMN_NAME, true, fieldType)
              ), 
              true);

      /* Then */
      assertThatUniqueIndexExists(dslContext,
          DATABASE_SCHEMA_NAME, ROOT_TABLE_NAME, ROOT_INDEX_NAME,
          ImmutableList.of(
              new Tuple3<String,Boolean,FieldType>(FIELD_COLUMN_NAME, true, fieldType)
            ));
    });
  }
  
}
