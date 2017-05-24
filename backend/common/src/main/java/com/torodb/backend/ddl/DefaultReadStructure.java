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

package com.torodb.backend.ddl;

import com.torodb.backend.BackendLoggerFactory;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.exceptions.InvalidDatabaseSchemaException;
import com.torodb.backend.meta.SchemaValidator;
import com.torodb.backend.meta.SchemaValidator.Table;
import com.torodb.backend.meta.SchemaValidator.TableField;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartIndexColumnTable;
import com.torodb.backend.tables.MetaDocPartIndexTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.backend.tables.MetaIndexFieldTable;
import com.torodb.backend.tables.MetaIndexTable;
import com.torodb.backend.tables.MetaScalarTable;
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
import com.torodb.core.TableRefFactory;
import com.torodb.core.exceptions.InvalidDatabaseException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaDocPartIndex;
import com.torodb.core.transaction.metainf.MutableMetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.WrapperMutableMetaSnapshot;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Result;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

public class DefaultReadStructure implements ReadStructureDdlOp {

  protected static final Logger LOGGER = BackendLoggerFactory.get(DefaultReadStructure.class);

  protected final SqlInterface sqlInterface;
  protected final SqlHelper sqlHelper;
  protected final TableRefFactory tableRefFactory;

  @Inject
  public DefaultReadStructure(SqlInterface sqlInterface, SqlHelper sqlHelper,
      TableRefFactory tableRefFactory) {
    this.sqlInterface = sqlInterface;
    this.sqlHelper = sqlHelper;
    this.tableRefFactory = tableRefFactory;
  }

  @Override
  public MetaSnapshot readMetadata(DSLContext dsl) {
    MutableMetaSnapshot mutableSnapshot = WrapperMutableMetaSnapshot.createEmpty();

    Updater updater = createUpdater(dsl);
    updater.loadMetaSnapshot(mutableSnapshot);

    return mutableSnapshot;
  }

  @Override
  public void close() throws Exception {
  }

  protected Updater createUpdater(DSLContext dsl) {
    return new Updater(dsl, tableRefFactory, sqlInterface);
  }

  protected static class Updater {

    protected final DSLContext dsl;
    protected final TableRefFactory tableRefFactory;
    protected final SqlInterface sqlInterface;
    protected final MetaCollectionTable<MetaCollectionRecord> collectionTable;
    protected final MetaDocPartTable<Object, MetaDocPartRecord<Object>> docPartTable;
    protected final MetaFieldTable<Object, MetaFieldRecord<Object>> fieldTable;
    protected final MetaScalarTable<Object, MetaScalarRecord<Object>> scalarTable;
    protected final MetaIndexTable<MetaIndexRecord> indexTable;
    protected final MetaIndexFieldTable<Object, MetaIndexFieldRecord<Object>> indexFieldTable;
    protected final MetaDocPartIndexTable<Object, MetaDocPartIndexRecord<Object>> docPartIndexTable;
    @SuppressWarnings("checkstyle:lineLength")
    protected final MetaDocPartIndexColumnTable<Object, MetaDocPartIndexColumnRecord<Object>> fieldIndexTable;

    public Updater(DSLContext dsl, TableRefFactory tableRefFactory, SqlInterface sqlInterface) {
      this.dsl = dsl;
      this.tableRefFactory = tableRefFactory;
      this.sqlInterface = sqlInterface;

      this.collectionTable = sqlInterface.getMetaDataReadInterface().getMetaCollectionTable();
      this.docPartTable = sqlInterface.getMetaDataReadInterface().getMetaDocPartTable();
      this.fieldTable = sqlInterface.getMetaDataReadInterface().getMetaFieldTable();
      this.scalarTable = sqlInterface.getMetaDataReadInterface().getMetaScalarTable();
      this.indexTable = sqlInterface.getMetaDataReadInterface().getMetaIndexTable();
      this.indexFieldTable = sqlInterface.getMetaDataReadInterface().getMetaIndexFieldTable();
      this.docPartIndexTable = sqlInterface.getMetaDataReadInterface().getMetaDocPartIndexTable();
      this.fieldIndexTable = sqlInterface.getMetaDataReadInterface()
          .getMetaDocPartIndexColumnTable();
    }

    private void loadMetaSnapshot(MutableMetaSnapshot mutableSnapshot) throws
        InvalidDatabaseSchemaException {

      MetaDatabaseTable<MetaDatabaseRecord> metaDatabaseTable = sqlInterface
          .getMetaDataReadInterface().getMetaDatabaseTable();
      Result<MetaDatabaseRecord> records =
          dsl.selectFrom(metaDatabaseTable)
              .fetch();

      for (MetaDatabaseRecord databaseRecord : records) {
        try {
          analyzeDatabase(mutableSnapshot, databaseRecord);
        } catch (SQLException sqlException) {
          throw new InvalidDatabaseException(sqlException);
        }
      }
    }

    private void analyzeDatabase(MutableMetaSnapshot snapshot, MetaDatabaseRecord databaseRecord)
        throws InvalidDatabaseSchemaException, SQLException {
      MutableMetaDatabase metaDatabase = snapshot.addMetaDatabase(databaseRecord.getName(),
          databaseRecord.getIdentifier());

      SchemaValidator schemaValidator = createSchemaValidator(databaseRecord);

      dsl.selectFrom(collectionTable)
          .where(collectionTable.DATABASE.eq(databaseRecord.getName()))
          .fetch()
          .forEach(
              (col) -> analyzeCollection(metaDatabase, col, schemaValidator));

      checkCompleteness(databaseRecord, schemaValidator);
    }

    protected SchemaValidator createSchemaValidator(MetaDatabaseRecord databaseRecord) {
      return new SchemaValidator(dsl, databaseRecord.getIdentifier(),
          databaseRecord.getName());
    }

    private void checkCompleteness(MetaDatabaseRecord database, SchemaValidator schemaValidator) {
      Map<String, MetaDocPartRecord<Object>> docParts = dsl
          .selectFrom(docPartTable)
          .where(docPartTable.DATABASE.eq(database.getName()))
          .fetchMap(docPartTable.IDENTIFIER);
      List<MetaFieldRecord<Object>> fields = dsl
          .selectFrom(fieldTable)
          .where(fieldTable.DATABASE.eq(database.getName()))
          .fetch();
      List<MetaScalarRecord<Object>> scalars = dsl
          .selectFrom(scalarTable)
          .where(scalarTable.DATABASE.eq(database.getName()))
          .fetch();
      for (Table table : schemaValidator.getExistingTables()) {
        MetaDocPartRecord<?> docPart = docParts.get(table.getName());
        if (docPart == null) {
          throw new InvalidDatabaseSchemaException(database.getIdentifier(), "Table " + getTableRef(
              database, table)
              + " has no container associated for database " + database.getName());
        }

        for (TableField existingField : table.fields()) {
          if (!sqlInterface.getIdentifierConstraints().isAllowedColumnIdentifier(existingField
              .getName())) {
            continue;
          }
          if (!SchemaValidator.containsField(existingField, docPart.getCollection(),
              docPart.getTableRefValue(tableRefFactory), fields, scalars, tableRefFactory)) {
            throw new InvalidDatabaseSchemaException(database.getIdentifier(),
                "Column " + getColumnRef(database, table, existingField)
                + " has no field associated for database " + database.getName());
          }
        }
      }
    }

    private void analyzeCollection(MutableMetaDatabase database, MetaCollectionRecord collection,
        SchemaValidator schemaValidator) {
      MutableMetaCollection col = database.addMetaCollection(
          collection.getName(),
          collection.getIdentifier()
      );

      dsl.selectFrom(docPartTable)
          .where(docPartTable.DATABASE.eq(database.getName())
              .and(docPartTable.COLLECTION.eq(collection.getName())))
          .fetch()
          .forEach(
              (docPart) -> analyzeDocPart(database, col, docPart, schemaValidator));

      dsl.selectFrom(indexTable)
          .where(indexTable.DATABASE.eq(database.getName())
              .and(indexTable.COLLECTION.eq(collection.getName())))
          .fetch()
          .forEach(
              (index) -> analyzeIndex(database, col, index, schemaValidator));
    }

    private void analyzeDocPart(MutableMetaDatabase database,
        MutableMetaCollection collection, MetaDocPartRecord<Object> docPartRecord,
        SchemaValidator schemaValidator) {
      if (!docPartRecord.getCollection().equals(collection.getName())) {
        return;
      }

      if (!schemaValidator.existsTable(docPartRecord.getIdentifier())) {
        throw new InvalidDatabaseSchemaException(database.getIdentifier(),
            "Doc part " + getDocPartRef(database, collection, docPartRecord)
            + " is associated with table " + getTableRef(database, docPartRecord)
            + " but there is no table with that name in the schema");
      }

      MutableMetaDocPart docPart = collection.addMetaDocPart(
          docPartRecord.getTableRefValue(tableRefFactory), docPartRecord.getIdentifier());
      dsl.selectFrom(fieldTable)
          .where(fieldTable.DATABASE.eq(database.getName())
              .and(fieldTable.COLLECTION.eq(collection.getName()))
              .and(fieldTable.TABLE_REF.eq(docPartRecord.getTableRef())))
          .fetch()
          .forEach(
              (field) -> analyzeField(database, collection, docPart, field, schemaValidator));

      dsl.selectFrom(scalarTable)
          .where(scalarTable.DATABASE.eq(database.getName())
              .and(scalarTable.COLLECTION.eq(collection.getName()))
              .and(scalarTable.TABLE_REF.eq(docPartRecord.getTableRef())))
          .fetch()
          .forEach(
              (scalar) -> analyzeScalar(database, collection, docPart, scalar, schemaValidator));

      dsl.selectFrom(docPartIndexTable)
          .where(docPartIndexTable.DATABASE.eq(database.getName())
              .and(docPartIndexTable.COLLECTION.eq(collection.getName()))
              .and(docPartIndexTable.TABLE_REF.eq(docPartRecord.getTableRef())))
          .fetch()
          .forEach(
              (docPartIndex) -> analyzeDocPartIndex(database, collection, docPart, docPartIndex,
                  schemaValidator));
    }

    private void analyzeField(MutableMetaDatabase database, MetaCollection collection,
        MutableMetaDocPart docPart,
        MetaFieldRecord<?> field, SchemaValidator schemaValidator) {

      if (!docPart.getTableRef().equals(field.getTableRefValue(tableRefFactory))) {
        return;
      }

      docPart.addMetaField(field.getName(), field.getIdentifier(), field.getType());

      if (!schemaValidator.existsColumn(docPart.getIdentifier(), field.getIdentifier())) {
        throw new InvalidDatabaseSchemaException(database.getIdentifier(),
            "Field " + getFieldRef(database, collection, docPart, field)
            + " is associated with column " + field.getIdentifier()
            + " but there is no column with that name in the table");
      }

      //TODO: some types can not be recognized using meta data
      if (!schemaValidator.existsColumnWithType(docPart.getIdentifier(), field.getIdentifier(),
          sqlInterface.getDataTypeProvider().getDataType(field.getType()))) {
        Table existingTable = schemaValidator.getTable(docPart.getIdentifier());
        TableField existingColumn = schemaValidator.getColumn(
            docPart.getIdentifier(), field.getIdentifier());
        throw new InvalidDatabaseSchemaException(database.getIdentifier(),
            "Field " + getFieldRef(database, collection, docPart, field)
            + " is associated with column " + getColumnRef(database, docPart, field)
            + " but existing column has a different type " + getColumnRef(
                database.getIdentifier(), existingTable, existingColumn));
      }
    }

    private void analyzeScalar(MutableMetaDatabase database, MetaCollection collection,
        MutableMetaDocPart docPart,
        MetaScalarRecord<?> scalar, SchemaValidator schemaValidator) {
      if (!docPart.getTableRef().equals(scalar.getTableRefValue(tableRefFactory))) {
        return;
      }

      docPart.addMetaScalar(scalar.getIdentifier(), scalar.getType());

      if (!schemaValidator.existsColumn(docPart.getIdentifier(), scalar.getIdentifier())) {
        throw new InvalidDatabaseSchemaException(database.getIdentifier(),
            "Scalar " + getScalarRef(database, collection, docPart, scalar)
            + " is associated with column " + getColumnRef(database, docPart, scalar)
            + " but there is no column with that name in the table");
      }

      //TODO: some types can not be recognized using meta data
      if (!schemaValidator.existsColumnWithType(docPart.getIdentifier(), scalar.getIdentifier(),
          sqlInterface.getDataTypeProvider().getDataType(scalar.getType()))) {
        String existingType = schemaValidator.getColumn(
            docPart.getIdentifier(), scalar.getIdentifier())
            .getTypeName();
        throw new InvalidDatabaseSchemaException(database.getIdentifier(),
            "Scalar " + getScalarRef(database, collection, docPart, scalar)
            + " is associated with column " + getColumnRef(database, docPart, scalar)
            + " but existing column has a different type " + existingType);
      }
    }

    private void analyzeDocPartIndex(MutableMetaDatabase database, MetaCollection collection,
        MutableMetaDocPart docPart, MetaDocPartIndexRecord<Object> docPartIndex,
        SchemaValidator schemaValidator) {
      TableRef tableRef = docPartIndex.getTableRefValue(tableRefFactory);

      if (!tableRef.equals(docPart.getTableRef())) {
        return;
      }

      if (!schemaValidator.existsIndex(docPartIndex.getIdentifier())) {
        throw new InvalidDatabaseSchemaException(database.getIdentifier(),
            "Doc part index under " + getDocPartRef(database, collection, docPart)
            + " is associated with index " + getIndexRef(database, docPart, docPartIndex)
            + " but there is no index with that name in the schema");
      }

      MutableMetaDocPartIndex metaDocPartIndex = docPart.addMetaDocPartIndex(docPartIndex
          .getUnique());

      dsl.selectFrom(fieldIndexTable)
          .where(fieldIndexTable.DATABASE.eq(database.getName())
              .and(fieldIndexTable.INDEX_IDENTIFIER.eq(docPartIndex.getIdentifier())))
          .orderBy(fieldIndexTable.POSITION)
          .fetch()
          .forEach(
              (indexField) -> analyzeDocPartIndexColumn(database, collection, docPart,
                  docPartIndex.getIdentifier(), metaDocPartIndex, indexField, schemaValidator));
      metaDocPartIndex.immutableCopy(docPartIndex.getIdentifier());
    }

    private void analyzeDocPartIndexColumn(MutableMetaDatabase database, MetaCollection collection,
        MetaDocPart docPart,
        String docPartIndexIdentifier, MutableMetaDocPartIndex docPartIndex,
        MetaDocPartIndexColumnRecord<Object> indexColumn,
        SchemaValidator schemaValidator) {
      if (!indexColumn.getIndexIdentifier().equals(docPartIndexIdentifier)) {
        return;
      }

      MetaField field = docPart.getMetaFieldByIdentifier(indexColumn.getIdentifier());
      if (field == null) {
        throw new InvalidDatabaseSchemaException(database.getIdentifier(),
            "Found doc part index column " + getDocPartIndexColumnRef(database, collection, docPart,
                indexColumn)
            + " but no associated field has been found");
      }

      if (!schemaValidator.existsIndexColumn(docPartIndexIdentifier, indexColumn.getPosition(),
          field.getIdentifier())) {
        throw new InvalidDatabaseSchemaException(database.getIdentifier(),
            "Doc part index column " + getDocPartIndexColumnRef(database, collection, docPart,
                indexColumn)
            + " is associated with field " + getFieldRef(database, collection, docPart, field)
            + " but there is no column with that name in index " + getIndexRef(database, docPart,
                docPartIndexIdentifier));
      }

      docPartIndex.putMetaDocPartIndexColumn(indexColumn.getPosition(), indexColumn.getIdentifier(),
          indexColumn.getOrdering());
    }

    private void analyzeIndex(MutableMetaDatabase db,
        MutableMetaCollection metaCollection, MetaIndexRecord index,
        SchemaValidator schemaValidator) {
      if (!index.getCollection().equals(metaCollection.getName())) {
        return;
      }
      MutableMetaIndex metaIndex = metaCollection.addMetaIndex(index.getName(), index.getUnique());

      dsl.selectFrom(indexFieldTable)
          .where(indexFieldTable.DATABASE.eq(db.getName())
              .and(indexFieldTable.COLLECTION.eq(metaCollection.getName()))
              .and(indexFieldTable.INDEX.eq(index.getName())))
          .orderBy(indexFieldTable.POSITION)
          .fetch()
          .forEach(
              (indexField) -> analyzeIndexField(metaIndex, indexField));

    }

    private void analyzeIndexField(
        MutableMetaIndex metaIndex, MetaIndexFieldRecord<Object> indexField) {
      if (!indexField.getIndex().equals(metaIndex.getName())) {
        return;
      }
      TableRef tableRef = indexField.getTableRefValue(tableRefFactory);
      metaIndex.addMetaIndexField(tableRef, indexField.getName(), indexField.getOrdering());
    }

    private String getDocPartRef(MetaDatabase database, MetaCollection collection,
        MetaDocPart docPart) {
      return database.getName() + "." + collection.getName() + ".[" + docPart.getTableRef() + "]";
    }

    private String getDocPartRef(MetaDatabase database, MetaCollection collection,
        MetaDocPartRecord<?> docPart) {
      return database.getName() + "." + collection.getName() + ".[" + docPart.getTableRefValue(
          tableRefFactory) + "]";
    }

    private String getFieldRef(MetaDatabase database, MetaCollection collection, 
        MetaDocPart docPart, MetaFieldRecord<?> field) {
      return getDocPartRef(database, collection, docPart) + "." + field.getName() + " (type:"
          + field.getType().name() + ")";
    }

    private String getFieldRef(MetaDatabase database, MetaCollection collection, 
        MetaDocPart docPart, MetaField field) {
      return getDocPartRef(database, collection, docPart) + "." + field.getName() + " (type:"
          + field.getType().name() + ")";
    }

    private String getScalarRef(MetaDatabase database, MetaCollection collection,
        MetaDocPart docPart, MetaScalarRecord<?> scalar) {
      return getDocPartRef(database, collection, docPart) + "[] (type:" + scalar.getType().name()
          + ")";
    }

    private String getDocPartIndexColumnRef(MetaDatabase database, MetaCollection collection,
        MetaDocPart docPart, MetaDocPartIndexColumnRecord<?> column) {
      return getDocPartRef(database, collection, docPart) + "." + column.getIdentifier();
    }

    private String getTableRef(MetaDatabase database, MetaDocPartRecord<?> docPart) {
      return database.getIdentifier() + "." + docPart.getIdentifier();
    }

    private String getTableRef(MetaDatabase database, MetaDocPart docPart) {
      return database.getIdentifier() + "." + docPart.getIdentifier();
    }

    private String getTableRef(MetaDatabaseRecord database, Table table) {
      return getTableRef(database.getIdentifier(), table);
    }

    private String getTableRef(String database, Table table) {
      return database + "." + table.getName();
    }

    private String getColumnRef(MetaDatabaseRecord database, Table table, TableField field) {
      return getColumnRef(database.getIdentifier(), table, field);
    }

    private String getColumnRef(String database, Table table, TableField field) {
      return getTableRef(database, table) + "." + field.getName() + "." + field.getName()
        + " (type:" + field.getTypeName() + ", sqlType:" + field.getSqlType() + ")";
    }

    private String getColumnRef(MetaDatabase database, MetaDocPart docPart,
        MetaFieldRecord<?> field) {
      DataType<?> dataType = sqlInterface.getDataTypeProvider().getDataType(field.getType());
      String type = dataType.getTypeName();
      int sqlType = dataType.getSQLType();
      return getTableRef(database, docPart) + "." + field.getIdentifier() + " (type:" + type
          + ", sqlType:" + sqlType + ")";
    }

    private String getColumnRef(MetaDatabase database, MetaDocPart docPart,
        MetaScalarRecord<?> scalar) {
      DataType<?> dataType = sqlInterface.getDataTypeProvider().getDataType(scalar.getType());
      String type = dataType.getTypeName();
      int sqlType = dataType.getSQLType();
      return getTableRef(database, docPart) + "." + scalar.getIdentifier() + " (type:" + type
          + ", sqlType:" + sqlType + ")";
    }

    private String getIndexRef(MetaDatabase database, MetaDocPart docPart,
        MetaDocPartIndexRecord<?> docPartIndex) {
      return database.getIdentifier() + "." + docPart.getIdentifier() + "." + docPartIndex
          .getIdentifier();
    }

    private String getIndexRef(MetaDatabase database, MetaDocPart docPart,
        String docPartIndexIdentifier) {
      return database.getIdentifier() + "." + docPart.getIdentifier() + "."
          + docPartIndexIdentifier;
    }
  }

}
