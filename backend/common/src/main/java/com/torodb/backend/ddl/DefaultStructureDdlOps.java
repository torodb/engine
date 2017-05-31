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
import com.torodb.backend.SqlInterface;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.exceptions.InvalidDatabaseException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaDocPartIndex;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import javax.inject.Inject;



public class DefaultStructureDdlOps implements WriteStructureDdlOps {

  private static final Logger LOGGER = BackendLoggerFactory.get(DefaultStructureDdlOps.class);

  private final SqlInterface sqlInterface;
  private final TableRefFactory tableRefFactory;
  private final IdentifierFactory identifierFactory;
  private final SchemaUpdater schemaUpdater;

  @Inject
  public DefaultStructureDdlOps(SqlInterface sqlInterface, TableRefFactory tableRefFactory,
      IdentifierFactory identifierFactory, SchemaUpdater schemaUpdater) {
    this.sqlInterface = sqlInterface;
    this.tableRefFactory = tableRefFactory;
    this.identifierFactory = identifierFactory;
    this.schemaUpdater = schemaUpdater;
  }

  @Override
  public void addDatabase(DSLContext dsl, MetaDatabase db) throws RollbackException {
    sqlInterface.getMetaDataWriteInterface().addMetaDatabase(dsl, db);
    sqlInterface.getStructureInterface().createDatabase(
        dsl, db.getIdentifier());
  }

  @Override
  public void dropDatabase(DSLContext dsl, MetaDatabase db) throws RollbackException {

    Iterator<? extends MetaCollection> metaCollectionIterator = db.streamMetaCollections()
        .iterator();
    while (metaCollectionIterator.hasNext()) {
      MetaCollection metaCollection = metaCollectionIterator.next();
      sqlInterface.getMetaDataWriteInterface().deleteMetaCollection(dsl, db, metaCollection);
    }
    sqlInterface.getMetaDataWriteInterface().deleteMetaDatabase(dsl, db);
    sqlInterface.getStructureInterface().dropDatabase(dsl, db);
    
  }

  @Override
  public void addCollection(DSLContext dsl, MetaDatabase db,
      MetaCollection newCol) throws RollbackException {

    sqlInterface.getMetaDataWriteInterface().addMetaCollection(dsl, db, newCol);
    TableRef rootTableRef = tableRefFactory.createRoot();
    MetaDocPart newRootDocPart = newCol.getMetaDocPartByTableRef(rootTableRef);
    if (newRootDocPart != null) {
      sqlInterface.getMetaDataWriteInterface().addMetaDocPart(dsl, db, newCol,
          newRootDocPart);
      sqlInterface.getStructureInterface().createRootDocPartTable(dsl,
          db.getIdentifier(), newRootDocPart.getIdentifier(), rootTableRef);
      sqlInterface.getStructureInterface().streamRootDocPartTableIndexesCreation(
          db.getIdentifier(), newRootDocPart.getIdentifier(), rootTableRef)
        .forEach(statement -> statement.apply(dsl));
    }
    
  }

  @Override
  public void dropCollection(DSLContext dsl, MetaDatabase db,
      MetaCollection coll) throws RollbackException {

    sqlInterface.getMetaDataWriteInterface().deleteMetaCollection(dsl, db, coll);
    sqlInterface.getStructureInterface().dropCollection(dsl, db.getIdentifier(), coll);
  }

  @Override
  public void addDocPart(DSLContext dsl, MetaDatabase db,
      MetaCollection col, MutableMetaDocPart newDocPart, boolean addColumns) throws
      RollbackException, UserException {
    sqlInterface.getMetaDataWriteInterface().addMetaDocPart(dsl, db, col,
        newDocPart);

    TableRef tableRef = newDocPart.getTableRef();
    if (tableRef.isRoot()) {
      sqlInterface.getStructureInterface().createRootDocPartTable(dsl, db.getIdentifier(),
          newDocPart.getIdentifier(), tableRef);
      sqlInterface.getStructureInterface().streamRootDocPartTableIndexesCreation(
          db.getIdentifier(), newDocPart.getIdentifier(), tableRef)
          .forEach(consumer -> {
            String index = consumer.apply(dsl);
            LOGGER.info("Created internal index {} for table {}", index,
                newDocPart.getIdentifier());
          });
    } else {
      sqlInterface.getStructureInterface().createDocPartTable(dsl, db.getIdentifier(),
          newDocPart.getIdentifier(), tableRef,
          col.getMetaDocPartByTableRef(tableRef.getParent().get()).getIdentifier());
      sqlInterface.getStructureInterface()
          .streamDocPartTableIndexesCreation(db.getIdentifier(), newDocPart.getIdentifier(),
              tableRef,
              col.getMetaDocPartByTableRef(tableRef.getParent().get()).getIdentifier())
          .forEach(consumer -> {
            String index = consumer.apply(dsl);
            LOGGER.info("Created internal index {} for table {}", index,
                newDocPart.getIdentifier());
          });
    }

    if (addColumns) {
      addColumns(dsl, db, col, newDocPart, newDocPart.streamScalars(), newDocPart.streamFields());
    }
  }

  @Override
  public void addColumns(DSLContext dsl, MetaDatabase db,
      MetaCollection col, MutableMetaDocPart docPart,
      Stream<? extends MetaScalar> scalars,
      Stream<? extends MetaField> fields) throws UserException, RollbackException {

    scalars.forEach(scalar -> this.addScalar(dsl, db, col, docPart, scalar));

    //addScalar throws checked exceptions, we cannot use streams there
    Iterator<? extends MetaField> fieldIt = fields.iterator();
    while (fieldIt.hasNext()) {
      addField(dsl, db, col, docPart, fieldIt.next());
    }
    
  }

  @Override
  public void checkOrCreateMetaDataTables(DSLContext dsl) throws InvalidDatabaseException {
    schemaUpdater.checkOrCreate(dsl);
  }

  private void addField(DSLContext dsl, MetaDatabase db, MetaCollection col, 
      MutableMetaDocPart docPart, MetaField newField) throws UserException {
    sqlInterface.getMetaDataWriteInterface().addMetaField(dsl, db, col, docPart,
        newField);
    sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, db.getIdentifier(),
        docPart.getIdentifier(), newField.getIdentifier(), sqlInterface.getDataTypeProvider()
        .getDataType(newField.getType()));

    List<Tuple2<MetaIndex, List<String>>> missingIndexes = col.getMissingIndexesForNewField(docPart,
        newField);

    for (Tuple2<MetaIndex, List<String>> missingIndexEntry : missingIndexes) {
      MetaIndex missingIndex = missingIndexEntry.v1();
      List<String> identifiers = missingIndexEntry.v2();

      MutableMetaDocPartIndex docPartIndex = docPart
          .getOrCreatePartialMutableDocPartIndexForMissingIndexAndNewField(
              missingIndex, identifiers, newField);

      if (missingIndex.isMatch(docPart, identifiers, docPartIndex)) {
        List<Tuple3<String, Boolean, FieldType>> columnList = new ArrayList<>(docPartIndex.size());
        for (String identifier : identifiers) {
          MetaDocPartIndexColumn docPartIndexColumn = docPartIndex
              .getMetaDocPartIndexColumnByIdentifier(identifier);
          columnList.add(new Tuple3<>(docPartIndexColumn.getIdentifier(), docPartIndexColumn
              .getOrdering().isAscending(),
              docPart.getMetaFieldByIdentifier(identifier).getType()));
        }
        MetaIdentifiedDocPartIndex identifiedDocPartIndex = docPartIndex.immutableCopy(
            identifierFactory.toIndexIdentifier(db, docPart.getIdentifier(), columnList));

        sqlInterface.getMetaDataWriteInterface()
            .addMetaDocPartIndex(dsl, db, col, docPart, identifiedDocPartIndex);

        for (String identifier : identifiers) {
          MetaDocPartIndexColumn docPartIndexColumn = docPartIndex
              .getMetaDocPartIndexColumnByIdentifier(identifier);
          sqlInterface.getMetaDataWriteInterface().addMetaDocPartIndexColumn(dsl, db, col,
              docPart, identifiedDocPartIndex, docPartIndexColumn);
        }

        sqlInterface.getStructureInterface().createIndex(dsl, identifiedDocPartIndex
            .getIdentifier(), db.getIdentifier(),
            docPart.getIdentifier(), columnList, docPartIndex.isUnique());
        LOGGER.info("Created index {} for table {} associated to logical index {}.{}.{}",
            identifiedDocPartIndex.getIdentifier(), docPart.getIdentifier(), db.getName(), col
            .getName(), missingIndex.getName());
      }
    }
  }

  private void addScalar(DSLContext dsl, MetaDatabase db, MetaCollection col, MetaDocPart docPart,
      MetaScalar newScalar) {
    sqlInterface.getMetaDataWriteInterface().addMetaScalar(dsl, db, col, docPart,
        newScalar);
    sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, db.getIdentifier(),
        docPart.getIdentifier(),
        newScalar.getIdentifier(), sqlInterface.getDataTypeProvider().getDataType(newScalar
        .getType()));
  }

  @Override
  public void close() throws Exception {
  }

}