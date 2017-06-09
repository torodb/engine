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
import com.torodb.backend.tables.records.KvRecord;
import com.torodb.backend.tables.records.MetaCollectionRecord;
import com.torodb.backend.tables.records.MetaDatabaseRecord;
import com.torodb.backend.tables.records.MetaDocPartIndexColumnRecord;
import com.torodb.backend.tables.records.MetaDocPartIndexRecord;
import com.torodb.backend.tables.records.MetaDocPartRecord;
import com.torodb.backend.tables.records.MetaFieldRecord;
import com.torodb.backend.tables.records.MetaIndexFieldRecord;
import com.torodb.backend.tables.records.MetaIndexRecord;
import com.torodb.backend.tables.records.MetaScalarRecord;
import com.torodb.core.TableRef;
import com.torodb.core.backend.MetaInfoKey;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.ImmutableMetaIndex;
import com.torodb.core.transaction.metainf.ImmutableMetaIndexField;
import com.torodb.core.transaction.metainf.ImmutableMetaScalar;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaIndexField;
import com.torodb.core.transaction.metainf.MetaScalar;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.jupiter.api.Test;

public abstract class AbstractMetaDataIntegrationSuite extends AbstractBackendIntegrationSuite {

  @Test
  public void metaDatabaseTableCanBeWrittenAndRead() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      MetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_NAME, DATABASE_SCHEMA_NAME).build();

      context.getSqlInterface().getMetaDataWriteInterface()
        .addMetaDatabase(dslContext, metaDatabase);

      Result<MetaDatabaseRecord> records = getMetaDatabaseTableRecords(dslContext);

      assertEquals(1, records.size());

      MetaDatabaseRecord firstRecord = records.get(0);

      assertEquals(DATABASE_NAME, firstRecord.getName());
      assertEquals(DATABASE_SCHEMA_NAME, firstRecord.getIdentifier());
    });
  }

  @Test
  public void metaCollectionTableCanBeWrittenAndRead() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      MetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_NAME, DATABASE_SCHEMA_NAME).build();
      MetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER).build();

      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaCollection(dslContext, metaDatabase, metaCollection);

      Result<MetaCollectionRecord> records = getMetaCollectionTableRecords(dslContext);

      assertEquals(1, records.size());

      MetaCollectionRecord firstRecord = records.get(0);

      assertEquals(COLLECTION_NAME, firstRecord.getName());
      assertEquals(COLLECTION_IDENTIFIER, firstRecord.getIdentifier());
      assertEquals(DATABASE_NAME, firstRecord.getDatabase());
    });
  }

  @Test
  public void metaDocPartTableCanBeWrittenAndRead() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      TableRef rootTableRef = context.getTableRefFactory().createRoot();

      MetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_NAME, DATABASE_SCHEMA_NAME).build();
      MetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER).build();
      MetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(rootTableRef, ROOT_TABLE_NAME).build();

      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDocPart(dslContext, metaDatabase, metaCollection, metaDocPart);

      Result<MetaDocPartRecord<Object>> records = getMetaDocPartTableRecords(dslContext);

      assertEquals(1, records.size());

      MetaDocPartRecord<Object> firstRecord = records.get(0);

      assertEquals(DATABASE_NAME, firstRecord.getDatabase());
      assertEquals(COLLECTION_NAME, firstRecord.getCollection());
      assertEquals(rootTableRef, firstRecord.getTableRefValue(context.getTableRefFactory()));
      assertEquals(ROOT_TABLE_NAME, firstRecord.getIdentifier());
    });
  }

  @Test
  public void metaFieldTableCanBeWrittenAndRead() throws Exception {
    FieldType fieldType = FieldType.INTEGER;
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      TableRef rootTableRef = context.getTableRefFactory().createRoot();

      MetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_NAME, DATABASE_SCHEMA_NAME).build();
      MetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER).build();
      MetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(rootTableRef, ROOT_TABLE_NAME).build();
      MetaField metaField = new ImmutableMetaField(FIELD_NAME, FIELD_COLUMN_NAME, fieldType);

      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaField(dslContext, metaDatabase, metaCollection, metaDocPart, metaField);

      Result<MetaFieldRecord<Object>> records = getMetaFieldTableRecords(dslContext);

      assertEquals(1, records.size());

      MetaFieldRecord<Object> firstRecord = records.get(0);

      assertEquals(DATABASE_NAME, firstRecord.getDatabase());
      assertEquals(COLLECTION_NAME, firstRecord.getCollection());
      assertEquals(rootTableRef, firstRecord.getTableRefValue(context.getTableRefFactory()));
      assertEquals(FIELD_NAME, firstRecord.getName());
      assertEquals(FIELD_COLUMN_NAME, firstRecord.getIdentifier());
      assertEquals(fieldType, firstRecord.getType());
    });
  }

  @Test
  public void metaScalarTableCanBeWrittenAndRead() throws Exception {
    FieldType fieldType = FieldType.INTEGER;
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      TableRef rootTableRef = context.getTableRefFactory().createRoot();

      MetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_NAME, DATABASE_SCHEMA_NAME).build();
      MetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER).build();
      MetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(rootTableRef, ROOT_TABLE_NAME).build();
      MetaScalar metaScalar = new ImmutableMetaScalar(SCALAR_COLUMN_NAME, fieldType);

      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaScalar(dslContext, metaDatabase, metaCollection, metaDocPart, metaScalar);

      Result<MetaScalarRecord<Object>> records = getMetaScalarTableRecords(dslContext);

      assertEquals(1, records.size());

      MetaScalarRecord<Object> firstRecord = records.get(0);

      assertEquals(DATABASE_NAME, firstRecord.getDatabase());
      assertEquals(COLLECTION_NAME, firstRecord.getCollection());
      assertEquals(rootTableRef, firstRecord.getTableRefValue(context.getTableRefFactory()));
      assertEquals(SCALAR_COLUMN_NAME, firstRecord.getIdentifier());
      assertEquals(fieldType, firstRecord.getType());
    });
  }

  @Test
  public void metaIndexTableCanBeWrittenAndRead() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      MetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_NAME, DATABASE_SCHEMA_NAME).build();
      MetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER).build();
      MetaIndex metaIndex = new ImmutableMetaIndex
          .Builder(INDEX_NAME, false).build();

      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaIndex(dslContext, metaDatabase, metaCollection, metaIndex);

      Result<MetaIndexRecord> records = getMetaIndexTableRecords(dslContext);

      assertEquals(1, records.size());

      MetaIndexRecord firstRecord = records.get(0);

      assertEquals(DATABASE_NAME, firstRecord.getDatabase());
      assertEquals(COLLECTION_NAME, firstRecord.getCollection());
      assertEquals(INDEX_NAME, firstRecord.getName());
      assertEquals(false, firstRecord.getUnique());
    });
  }

  @Test
  public void metaIndexFieldTableCanBeWrittenAndRead() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      TableRef rootTableRef = context.getTableRefFactory().createRoot();
      
      MetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_NAME, DATABASE_SCHEMA_NAME).build();
      MetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER).build();
      MetaIndex metaIndex = new ImmutableMetaIndex
          .Builder(INDEX_NAME, false).build();
      MetaIndexField metaIndexField = 
          new ImmutableMetaIndexField(0, rootTableRef, FIELD_NAME, FieldIndexOrdering.ASC);

      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaIndexField(dslContext, metaDatabase, metaCollection, metaIndex, metaIndexField);

      Result<MetaIndexFieldRecord<Object>> records = getMetaIndexFieldTableRecords(dslContext);

      assertEquals(1, records.size());

      MetaIndexFieldRecord<Object> firstRecord = records.get(0);

      assertEquals(DATABASE_NAME, firstRecord.getDatabase());
      assertEquals(COLLECTION_NAME, firstRecord.getCollection());
      assertEquals(INDEX_NAME, firstRecord.getIndex());
      assertEquals(FIELD_NAME, firstRecord.getName());
      assertEquals(Integer.valueOf(0), firstRecord.getPosition());
      assertEquals(FieldIndexOrdering.ASC, firstRecord.getOrdering());
    });
  }

  @Test
  public void metaDocPartIndexTableCanBeWrittenAndRead() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      TableRef rootTableRef = context.getTableRefFactory().createRoot();

      MetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_NAME, DATABASE_SCHEMA_NAME).build();
      MetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER).build();
      MetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(rootTableRef, ROOT_TABLE_NAME).build();
      MetaIdentifiedDocPartIndex metaDocPartIndex = 
          new ImmutableMetaIdentifiedDocPartIndex.Builder(ROOT_INDEX_NAME, false)
            .add(new ImmutableMetaDocPartIndexColumn(0, FIELD_COLUMN_NAME, FieldIndexOrdering.ASC))
            .build();

      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDocPartIndex(dslContext, metaDatabase, metaCollection, metaDocPart, 
              metaDocPartIndex);

      Result<MetaDocPartIndexRecord<Object>> records = 
          getMetaDocPartIndexTableRecords(dslContext);

      assertEquals(1, records.size());

      MetaDocPartIndexRecord<Object> firstRecord = records.get(0);

      assertEquals(DATABASE_NAME, firstRecord.getDatabase());
      assertEquals(COLLECTION_NAME, firstRecord.getCollection());
      assertEquals(rootTableRef, firstRecord.getTableRefValue(context.getTableRefFactory()));
      assertEquals(ROOT_INDEX_NAME, firstRecord.getIdentifier());
      assertEquals(false, firstRecord.getUnique());
    });
  }

  @Test
  public void metaDocPartIndexColumnTableCanBeWrittenAndRead() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      TableRef rootTableRef = context.getTableRefFactory().createRoot();

      MetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_NAME, DATABASE_SCHEMA_NAME).build();
      MetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER).build();
      MetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(rootTableRef, ROOT_TABLE_NAME).build();
      MetaIdentifiedDocPartIndex metaDocPartIndex = 
          new ImmutableMetaIdentifiedDocPartIndex.Builder(ROOT_INDEX_NAME, false)
            .build();
      MetaDocPartIndexColumn metaDocPartIndexColumn = 
          new ImmutableMetaDocPartIndexColumn(0, FIELD_COLUMN_NAME, FieldIndexOrdering.ASC);

      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDocPartIndexColumn(dslContext, metaDatabase, metaCollection, metaDocPart, 
              metaDocPartIndex, metaDocPartIndexColumn);

      Result<MetaDocPartIndexColumnRecord<Object>> records = 
          getMetaDocPartIndexColumnTableRecords(dslContext);

      assertEquals(1, records.size());

      MetaDocPartIndexColumnRecord<Object> firstRecord = records.get(0);

      assertEquals(DATABASE_NAME, firstRecord.getDatabase());
      assertEquals(COLLECTION_NAME, firstRecord.getCollection());
      assertEquals(rootTableRef, firstRecord.getTableRefValue(context.getTableRefFactory()));
      assertEquals(ROOT_INDEX_NAME, firstRecord.getIndexIdentifier());
      assertEquals(FIELD_COLUMN_NAME, firstRecord.getIdentifier());
      assertEquals(Integer.valueOf(0), firstRecord.getPosition());
      assertEquals(FieldIndexOrdering.ASC, firstRecord.getOrdering());
    });
  }

  @Test
  public void metaKvTableCanBeWrittenAndRead() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      MetaInfoKey metaInfoKey = () -> INFO_KEY_NAME;

      context.getSqlInterface().getMetaDataWriteInterface()
          .writeMetaInfo(dslContext, metaInfoKey, INFO_KEY_VALUE);

      Result<KvRecord> records = getKvTableRecords(dslContext);

      assertEquals(1, records.size());

      KvRecord firstRecord = records.get(0);

      assertEquals(INFO_KEY_NAME, firstRecord.getKey());
      assertEquals(INFO_KEY_VALUE, firstRecord.getValue());
    });
  }

  @Test
  public void metaDatabaseTableCanBeDeleted() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_NAME, DATABASE_SCHEMA_NAME)
          .build();

      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDatabase(dslContext, metaDatabase);

      context.getSqlInterface().getMetaDataWriteInterface()
          .deleteMetaDatabase(dslContext, metaDatabase);

      assertEquals(0, getMetaDatabaseTableRecords(dslContext).size());
      assertEquals(0, getMetaCollectionTableRecords(dslContext).size());
      assertEquals(0, getMetaIndexTableRecords(dslContext).size());
      assertEquals(0, getMetaIndexFieldTableRecords(dslContext).size());
      assertEquals(0, getMetaDocPartTableRecords(dslContext).size());
      assertEquals(0, getMetaFieldTableRecords(dslContext).size());
      assertEquals(0, getMetaScalarTableRecords(dslContext).size());
      assertEquals(0, getMetaDocPartIndexTableRecords(dslContext).size());
      assertEquals(0, getMetaDocPartIndexColumnTableRecords(dslContext).size());
    });
  }

  @Test
  public void metaCollectionTableCanBeCascadeDeleted() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      FieldType fieldType = FieldType.INTEGER;
      TableRef rootTableRef = context.getTableRefFactory().createRoot();

      ImmutableMetaIndexField metaIndexField = 
          new ImmutableMetaIndexField(0, rootTableRef, FIELD_NAME, FieldIndexOrdering.ASC);
      ImmutableMetaIndex metaIndex = new ImmutableMetaIndex
          .Builder(INDEX_NAME, false)
          .add(metaIndexField)
          .build();
      ImmutableMetaField metaField = 
          new ImmutableMetaField(FIELD_NAME, FIELD_COLUMN_NAME, fieldType);
      ImmutableMetaScalar metaScalar = new ImmutableMetaScalar(SCALAR_COLUMN_NAME, fieldType);
      ImmutableMetaDocPartIndexColumn metaDocPartIndexColumn = 
          new ImmutableMetaDocPartIndexColumn(0, FIELD_COLUMN_NAME, FieldIndexOrdering.ASC);
      ImmutableMetaIdentifiedDocPartIndex metaDocPartIndex = 
          new ImmutableMetaIdentifiedDocPartIndex.Builder(ROOT_INDEX_NAME, false)
            .add(metaDocPartIndexColumn)
            .build();
      ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(rootTableRef, ROOT_TABLE_NAME)
          .put(metaField)
          .put(metaScalar)
          .put(metaDocPartIndex)
          .build();
      ImmutableMetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER)
          .put(metaDocPart)
          .put(metaIndex)
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
          .addMetaIndex(dslContext, metaDatabase, metaCollection, metaIndex);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaIndexField(dslContext, metaDatabase, metaCollection, metaIndex,
              metaIndexField);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDocPart(dslContext, metaDatabase, metaCollection, metaDocPart);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaField(dslContext, metaDatabase, metaCollection, metaDocPart, 
              metaField);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaScalar(dslContext, metaDatabase, metaCollection, metaDocPart, 
              metaScalar);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDocPartIndex(dslContext, metaDatabase, metaCollection, metaDocPart, 
              metaDocPartIndex);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDocPartIndexColumn(dslContext, metaDatabase, metaCollection, metaDocPart, 
              metaDocPartIndex, metaDocPartIndexColumn);

      context.getSqlInterface().getMetaDataWriteInterface()
          .deleteMetaCollection(dslContext, metaDatabase, metaCollection);

      assertEquals(1, getMetaDatabaseTableRecords(dslContext).size());
      assertEquals(0, getMetaCollectionTableRecords(dslContext).size());
      assertEquals(0, getMetaIndexTableRecords(dslContext).size());
      assertEquals(0, getMetaIndexFieldTableRecords(dslContext).size());
      assertEquals(0, getMetaDocPartTableRecords(dslContext).size());
      assertEquals(0, getMetaFieldTableRecords(dslContext).size());
      assertEquals(0, getMetaScalarTableRecords(dslContext).size());
      assertEquals(0, getMetaDocPartIndexTableRecords(dslContext).size());
      assertEquals(0, getMetaDocPartIndexColumnTableRecords(dslContext).size());
    });
  }

  @Test
  public void metaIndexTableCanBeDeleted() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      FieldType fieldType = FieldType.INTEGER;
      TableRef rootTableRef = context.getTableRefFactory().createRoot();

      ImmutableMetaIndexField metaIndexField = 
          new ImmutableMetaIndexField(0, rootTableRef, FIELD_NAME, FieldIndexOrdering.ASC);
      ImmutableMetaIndex metaIndex = new ImmutableMetaIndex
          .Builder(INDEX_NAME, false)
          .add(metaIndexField)
          .build();
      ImmutableMetaField metaField = 
          new ImmutableMetaField(FIELD_NAME, FIELD_COLUMN_NAME, fieldType);
      ImmutableMetaScalar metaScalar = new ImmutableMetaScalar(SCALAR_COLUMN_NAME, fieldType);
      ImmutableMetaDocPartIndexColumn metaDocPartIndexColumn = 
          new ImmutableMetaDocPartIndexColumn(0, FIELD_COLUMN_NAME, FieldIndexOrdering.ASC);
      ImmutableMetaIdentifiedDocPartIndex metaDocPartIndex = 
          new ImmutableMetaIdentifiedDocPartIndex.Builder(ROOT_INDEX_NAME, false)
            .add(metaDocPartIndexColumn)
            .build();
      ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(rootTableRef, ROOT_TABLE_NAME)
          .put(metaField)
          .put(metaScalar)
          .put(metaDocPartIndex)
          .build();
      ImmutableMetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER)
          .put(metaDocPart)
          .put(metaIndex)
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
          .addMetaIndex(dslContext, metaDatabase, metaCollection, metaIndex);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaIndexField(dslContext, metaDatabase, metaCollection, metaIndex,
              metaIndexField);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDocPart(dslContext, metaDatabase, metaCollection, metaDocPart);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaField(dslContext, metaDatabase, metaCollection, metaDocPart, 
              metaField);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaScalar(dslContext, metaDatabase, metaCollection, metaDocPart, 
              metaScalar);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDocPartIndex(dslContext, metaDatabase, metaCollection, metaDocPart, 
              metaDocPartIndex);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDocPartIndexColumn(dslContext, metaDatabase, metaCollection, metaDocPart, 
              metaDocPartIndex, metaDocPartIndexColumn);

      context.getSqlInterface().getMetaDataWriteInterface()
          .deleteMetaIndex(dslContext, metaDatabase, metaCollection, metaIndex);

      assertEquals(1, getMetaDatabaseTableRecords(dslContext).size());
      assertEquals(1, getMetaCollectionTableRecords(dslContext).size());
      assertEquals(0, getMetaIndexTableRecords(dslContext).size());
      assertEquals(0, getMetaIndexFieldTableRecords(dslContext).size());
      assertEquals(1, getMetaDocPartTableRecords(dslContext).size());
      assertEquals(1, getMetaFieldTableRecords(dslContext).size());
      assertEquals(1, getMetaScalarTableRecords(dslContext).size());
      assertEquals(1, getMetaDocPartIndexTableRecords(dslContext).size());
      assertEquals(1, getMetaDocPartIndexColumnTableRecords(dslContext).size());
    });
  }

  @Test
  public void metaDocPartIndexTableCanBeDeleted() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      FieldType fieldType = FieldType.INTEGER;
      TableRef rootTableRef = context.getTableRefFactory().createRoot();

      ImmutableMetaIndexField metaIndexField = 
          new ImmutableMetaIndexField(0, rootTableRef, FIELD_NAME, FieldIndexOrdering.ASC);
      ImmutableMetaIndex metaIndex = new ImmutableMetaIndex
          .Builder(INDEX_NAME, false)
          .add(metaIndexField)
          .build();
      ImmutableMetaField metaField = 
          new ImmutableMetaField(FIELD_NAME, FIELD_COLUMN_NAME, fieldType);
      ImmutableMetaScalar metaScalar = new ImmutableMetaScalar(SCALAR_COLUMN_NAME, fieldType);
      ImmutableMetaDocPartIndexColumn metaDocPartIndexColumn = 
          new ImmutableMetaDocPartIndexColumn(0, FIELD_COLUMN_NAME, FieldIndexOrdering.ASC);
      ImmutableMetaIdentifiedDocPartIndex metaDocPartIndex = 
          new ImmutableMetaIdentifiedDocPartIndex.Builder(ROOT_INDEX_NAME, false)
            .add(metaDocPartIndexColumn)
            .build();
      ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart
          .Builder(rootTableRef, ROOT_TABLE_NAME)
          .put(metaField)
          .put(metaScalar)
          .put(metaDocPartIndex)
          .build();
      ImmutableMetaCollection metaCollection = new ImmutableMetaCollection
          .Builder(COLLECTION_NAME, COLLECTION_IDENTIFIER)
          .put(metaDocPart)
          .put(metaIndex)
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
          .addMetaIndex(dslContext, metaDatabase, metaCollection, metaIndex);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaIndexField(dslContext, metaDatabase, metaCollection, metaIndex,
              metaIndexField);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDocPart(dslContext, metaDatabase, metaCollection, metaDocPart);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaField(dslContext, metaDatabase, metaCollection, metaDocPart, 
              metaField);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaScalar(dslContext, metaDatabase, metaCollection, metaDocPart, 
              metaScalar);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDocPartIndex(dslContext, metaDatabase, metaCollection, metaDocPart, 
              metaDocPartIndex);
      context.getSqlInterface().getMetaDataWriteInterface()
          .addMetaDocPartIndexColumn(dslContext, metaDatabase, metaCollection, metaDocPart, 
              metaDocPartIndex, metaDocPartIndexColumn);

      context.getSqlInterface().getMetaDataWriteInterface()
          .deleteMetaDocPartIndex(dslContext, metaDatabase, metaCollection, 
              metaDocPart, metaDocPartIndex);

      assertEquals(1, getMetaDatabaseTableRecords(dslContext).size());
      assertEquals(1, getMetaCollectionTableRecords(dslContext).size());
      assertEquals(1, getMetaIndexTableRecords(dslContext).size());
      assertEquals(1, getMetaIndexFieldTableRecords(dslContext).size());
      assertEquals(1, getMetaDocPartTableRecords(dslContext).size());
      assertEquals(1, getMetaFieldTableRecords(dslContext).size());
      assertEquals(1, getMetaScalarTableRecords(dslContext).size());
      assertEquals(0, getMetaDocPartIndexTableRecords(dslContext).size());
      assertEquals(0, getMetaDocPartIndexColumnTableRecords(dslContext).size());
    });
  }

  private Result<MetaDatabaseRecord> getMetaDatabaseTableRecords(DSLContext dslContext) {
    MetaDatabaseTable<MetaDatabaseRecord> metaDatabaseTable = context.getSqlInterface()
        .getMetaDataReadInterface().getMetaDatabaseTable();
    return dslContext.selectFrom(metaDatabaseTable)
        .fetch();
  }

  private Result<MetaCollectionRecord> getMetaCollectionTableRecords(DSLContext dslContext) {
    MetaCollectionTable<MetaCollectionRecord> metaCollectionTable = context.getSqlInterface()
        .getMetaDataReadInterface().getMetaCollectionTable();
    return dslContext.selectFrom(metaCollectionTable)
        .fetch();
  }

  private Result<MetaDocPartRecord<Object>> getMetaDocPartTableRecords(DSLContext dslContext) {
    MetaDocPartTable<Object, MetaDocPartRecord<Object>> metaDocPartTable = context.getSqlInterface()
        .getMetaDataReadInterface().getMetaDocPartTable();

    return dslContext.selectFrom(metaDocPartTable)
        .fetch();
  }

  private Result<MetaFieldRecord<Object>> getMetaFieldTableRecords(DSLContext dslContext) {
    MetaFieldTable<Object, MetaFieldRecord<Object>> metaFieldTable = context.getSqlInterface()
        .getMetaDataReadInterface().getMetaFieldTable();

    return dslContext.selectFrom(metaFieldTable)
        .fetch();
  }

  private Result<MetaScalarRecord<Object>> getMetaScalarTableRecords(DSLContext dslContext) {
    MetaScalarTable<Object, MetaScalarRecord<Object>> metaScalarTable = context.getSqlInterface()
        .getMetaDataReadInterface().getMetaScalarTable();

    return dslContext.selectFrom(metaScalarTable)
        .fetch();
  }

  private Result<MetaIndexRecord> getMetaIndexTableRecords(DSLContext dslContext) {
    MetaIndexTable<MetaIndexRecord> metaIndexTable = context.getSqlInterface()
        .getMetaDataReadInterface().getMetaIndexTable();

    return dslContext.selectFrom(metaIndexTable)
        .fetch();
  }

  private Result<MetaIndexFieldRecord<Object>> getMetaIndexFieldTableRecords(
      DSLContext dslContext) {
    MetaIndexFieldTable<Object, MetaIndexFieldRecord<Object>> metaIndexFieldTable = 
        context.getSqlInterface().getMetaDataReadInterface().getMetaIndexFieldTable();

    return dslContext.selectFrom(metaIndexFieldTable)
        .fetch();
  }

  private Result<MetaDocPartIndexRecord<Object>> getMetaDocPartIndexTableRecords(
      DSLContext dslContext) {
    MetaDocPartIndexTable<Object, MetaDocPartIndexRecord<Object>> metaDocPartIndexTable = 
        context.getSqlInterface().getMetaDataReadInterface().getMetaDocPartIndexTable();

    return dslContext.selectFrom(metaDocPartIndexTable)
        .fetch();
  }

  private Result<MetaDocPartIndexColumnRecord<Object>> getMetaDocPartIndexColumnTableRecords(
      DSLContext dslContext) {
    MetaDocPartIndexColumnTable<Object, 
          MetaDocPartIndexColumnRecord<Object>> metaDocPartIndexColumnTable = 
        context.getSqlInterface().getMetaDataReadInterface().getMetaDocPartIndexColumnTable();

    return dslContext.selectFrom(metaDocPartIndexColumnTable)
        .fetch();
  }

  private Result<KvRecord> getKvTableRecords(
      DSLContext dslContext) {
    KvTable<KvRecord> kvTable = 
        context.getSqlInterface().getMetaDataReadInterface().getKvTable();

    return dslContext.selectFrom(kvTable)
        .fetch();
  }

}
