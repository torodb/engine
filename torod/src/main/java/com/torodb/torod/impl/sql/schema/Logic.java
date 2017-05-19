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

package com.torodb.torod.impl.sql.schema;

import com.google.common.base.Throwables;
import com.torodb.core.TableRef;
import com.torodb.core.backend.DdlOperationExecutor;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaIndex;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.TorodLoggerFactory;
import com.torodb.torod.exception.AlreadyExistentCollectionException;
import com.torodb.torod.exception.UnexistentCollectionException;
import com.torodb.torod.exception.UnexistentDatabaseException;
import com.torodb.torod.exception.UnsupportedIndexException;
import com.torodb.torod.exception.UserSchemaException;
import com.torodb.torod.impl.sql.schema.TransactionalMetadata.TransactionalSnapshot;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;


/**
 *
 */
@NotThreadSafe
class Logic {

  private static final Logger LOGGER = TorodLoggerFactory.get(Logic.class);

  private final IdentifierFactory idFactory;
  private final TransactionalMetadata metadata = new TransactionalMetadata();
  private final Function<AttributeReference, TableRef> tableRefTranslator;
  private final DocSchemaAnalyzer docSchemaAnalyzer;
  private final Supervisor supervisor;

  @Inject
  public Logic(
      IdentifierFactory idFactory,
      Function<AttributeReference, TableRef> tableRefTranslator,
      DocSchemaAnalyzer docSchemaAnalyzer,
      Supervisor supervisor) {
    this.idFactory = idFactory;
    this.tableRefTranslator = tableRefTranslator;
    this.docSchemaAnalyzer = docSchemaAnalyzer;
    this.supervisor = supervisor;
  }

  <E> E executeAtomically(Function<ImmutableMetaSnapshot, E> fun) {
    return fun.apply(metadata.getSnapshot());
  }

  void refreshMetadata(DdlOperationExecutor ops) {
    ImmutableMetaSnapshot newSnapshot = ops.readMetadata().immutableCopy();
    metadata.setSnapshot(newSnapshot);
  }

  boolean prepareSchema(DdlOperationExecutor ops, String dbName, String colName,
      Collection<KvDocument> docs) throws UserSchemaException {
    try (TransactionalSnapshot snapshot = metadata.createTransactionalSnapshot()) {
      MutableMetaDatabase db = getDbOrCreate(snapshot, ops, dbName);
      MutableMetaCollection col = getColOrCreate(snapshot, ops, db, colName);

      BatchMetaCollection batchCol = new BatchMetaCollection(col);
      docSchemaAnalyzer.analyze(db, batchCol, docs.stream());

      try {
        boolean changed = snapshot.hasChanged();
        adaptCollection(ops, db, batchCol);
        snapshot.commit();

        return changed;
      } catch (Throwable ex) {
        try {
          LOGGER.debug("Error while trying to prepare the schema... Refreshing metadata...", ex);
          refreshMetadata(ops);
          LOGGER.debug("Metadata correctly refreshed");
        } catch (Throwable ex2) {
          LOGGER.fatal("Fatal error while trying to refresh the metadata after an erroneuous "
              + "attempt to prepare the schema", ex2);
          SupervisorDecision decision = supervisor.onError(this, ex2);
          metadata.setSnapshot(null);
          switch (decision) {
            case IGNORE: {
              break;
            }
            default:
            case STOP: {
              throw new AssertionError("Unexpected error");
            }
          }
        }
        Throwables.throwIfUnchecked(ex);
        if (ex instanceof UserException) {
          throw new UserSchemaException((UserException) ex);
        }
        return false;
      }
    }
  }

  private void adaptCollection(DdlOperationExecutor op,
      MetaDatabase db, BatchMetaCollection col) throws UserException, RollbackException {
    for (BatchMetaDocPart dp : col.getOnBatchModifiedMetaDocParts()) {
      adaptDocPart(op, db, col, dp);
    }
  }

  private void adaptDocPart(
      DdlOperationExecutor op, MetaDatabase db, MetaCollection col,
      BatchMetaDocPart dp) throws UserException, RollbackException {
    if (dp.isCreatedOnCurrentBatch()) {
      op.addDocPart(db, col, dp, true);
    } else {
      op.addColumns(db, col, dp, dp.streamAddedMetaScalars(), dp.streamAddedMetaFields());
    }
  }

  void createDatabase(DdlOperationExecutor op, String dbName) {
    try (TransactionalSnapshot snapshot = metadata.createTransactionalSnapshot()) {
      getDbOrCreate(snapshot, op, dbName);
    }
  }

  void dropDatabase(DdlOperationExecutor ops, String dbName) {
    try (TransactionalSnapshot snapshot = metadata.createTransactionalSnapshot()) {
      MutableMetaDatabase metaDb = snapshot.getMetaDatabaseByName(dbName);
      if (metaDb != null) {
        ops.dropDatabase(metaDb);
        metaDb.removeMetaCollectionByName(dbName);
        snapshot.commit();
      }
    }
  }

  void createCollection(DdlOperationExecutor ops, String dbName, String colName)
      throws UnexistentDatabaseException {
    try (TransactionalSnapshot snapshot = metadata.createTransactionalSnapshot()) {
      MutableMetaDatabase metaDb = snapshot.getMetaDatabaseByName(dbName);
      if (metaDb == null) {
        throw new UnexistentDatabaseException(dbName);
      }
      getColOrCreate(snapshot, ops, metaDb, colName);
      snapshot.commit();
      
    }
  }

  void dropCollection(DdlOperationExecutor ops, String dbName, String colName)
      throws UnexistentDatabaseException {
    try (TransactionalSnapshot snapshot = metadata.createTransactionalSnapshot()) {
      MutableMetaDatabase metaDb = snapshot.getMetaDatabaseByName(dbName);
      if (metaDb == null) {
        throw new UnexistentDatabaseException(dbName);
      }

      MutableMetaCollection metaCol = metaDb.getMetaCollectionByName(colName);
      if (metaCol != null) {
        ops.dropCollection(metaDb, metaCol);
        metaDb.removeMetaCollectionByName(colName);
        snapshot.commit();
      }
      
    }
  }

  void renameCollection(DdlOperationExecutor ops, String fromDbName, String fromColName,
      String toDbName, String toColName) throws UnexistentDatabaseException,
      UnexistentCollectionException, AlreadyExistentCollectionException {
    try (TransactionalSnapshot snapshot = metadata.createTransactionalSnapshot()) {
      MutableMetaDatabase fromDb = getDbOrThrow(snapshot, fromDbName);
      MetaCollection fromCol = getColOrThrow(fromDb, fromColName);

      MutableMetaDatabase toDb = getDbOrCreate(snapshot, ops, toDbName);
      MutableMetaCollection toCol = toDb.getMetaCollectionByName(toColName);

      if (toCol != null) {
        throw new AlreadyExistentCollectionException(toDbName, toColName);
      } else {
        toCol = createCollectionPrivate(ops, snapshot, toDb, toColName);
      }

      ops.renameCollection(fromDb, fromCol, toDb, toCol);
      //TODO: Metainformation should be copied here... but for historical reasons
      //it is being copied on DdlOperationExecutor#renameCollection
      fromDb.removeMetaCollectionByName(fromColName);
      snapshot.commit();
    }
  }

  void disableDataImportMode(DdlOperationExecutor ops, String dbName) 
      throws UnexistentDatabaseException {
    try (TransactionalSnapshot snapshot = metadata.createTransactionalSnapshot()) {
      MutableMetaDatabase db = getDbOrThrow(snapshot, dbName);
      ops.disableDataImportMode(db);
      snapshot.commit();
    }
    
  }

  void enableDataImportMode(DdlOperationExecutor ops, String dbName)
      throws UnexistentDatabaseException {
    try (TransactionalSnapshot snapshot = metadata.createTransactionalSnapshot()) {
      MutableMetaDatabase db = getDbOrThrow(snapshot, dbName);
      ops.enableDataImportMode(db);
      snapshot.commit();
    } 
    
  }

  boolean createIndex(DdlOperationExecutor ops, String dbName, String colName, String indexName,
      List<IndexFieldInfo> fields, boolean unique) throws UnexistentDatabaseException,
      UnexistentCollectionException, UnsupportedIndexException {
    try (TransactionalSnapshot snapshot = metadata.createTransactionalSnapshot()) {
      if (fields.size() > 1) {
        throw new UnsupportedIndexException(dbName, colName, indexName,
                "Compound indexes are not supported");
      }

      MutableMetaDatabase metaDb = getDbOrThrow(snapshot, dbName);
      MutableMetaCollection metaColl = getColOrThrow(metaDb, colName);

      List<Tuple3<TableRef, String, FieldIndexOrdering>> indexFieldDefs = new ArrayList<>(
          fields.size());
      for (IndexFieldInfo field : fields) {
        AttributeReference attRef = field.getAttributeReference();
        FieldIndexOrdering ordering = field.isAscending() ? FieldIndexOrdering.ASC :
            FieldIndexOrdering.DESC;
        TableRef tableRef = tableRefTranslator.apply(attRef);
        String lastKey = extractKeyName(attRef.getKeys().get(attRef.getKeys().size() - 1));
        indexFieldDefs.add(new Tuple3<>(tableRef, lastKey, ordering));
      }

      if (unique) {
        TableRef anyIndexTableRef = indexFieldDefs.stream()
            .findAny().get().v1();
        boolean isUniqueIndexWithMutlipleTableRefs = indexFieldDefs.stream()
            .anyMatch(t -> !t.v1().equals(anyIndexTableRef));

        if (isUniqueIndexWithMutlipleTableRefs) {
          throw new UnsupportedIndexException(dbName, colName, indexName,
              "Unique index with multiple table ref are not supported");
        }
      }

      boolean indexExists = metaColl.streamContainedMetaIndexes()
          .anyMatch(index -> index.getName().equals(indexName) || (index.isUnique() == unique
              && index.size() == indexFieldDefs.size() && Seq.seq(index.iteratorFields())
              .allMatch(indexField -> {
                Tuple3<TableRef, String, FieldIndexOrdering> indexFieldDef =
                    indexFieldDefs.get(indexField.getPosition());
                return indexFieldDef != null && indexFieldDef.v1().equals(indexField.getTableRef())
                    && indexFieldDef.v2().equals(indexField.getFieldName()) && indexFieldDef.v3()
                    == indexField.getOrdering();
              })));

      if (!indexExists) {
        MutableMetaIndex metaIndex = metaColl.addMetaIndex(indexName, unique);
        for (Tuple3<TableRef, String, FieldIndexOrdering> indexFieldDef : indexFieldDefs) {
          metaIndex.addMetaIndexField(indexFieldDef.v1(), indexFieldDef.v2(), indexFieldDef.v3());
        }
        try {
          ops.createIndex(metaDb, metaColl, metaIndex);
          snapshot.commit();
        } catch (UserException ex) {
          throw new UserSchemaException("Impossible to create the index on the backend", ex);
        }
      }

      return indexExists;
    }
  }

  boolean dropIndex(DdlOperationExecutor ops, String dbName, String colName, String indexName) {
    try (TransactionalSnapshot snapshot = metadata.createTransactionalSnapshot()) {
      MutableMetaDatabase db = snapshot.getMetaDatabaseByName(dbName);
      if (db == null) {
        throw new UnexistentDatabaseException(dbName);
      }
      MutableMetaCollection col = db.getMetaCollectionByName(colName);
      if (col == null) {
        throw new UnexistentCollectionException(dbName, colName);
      }
      MetaIndex index = col.getMetaIndexByName(indexName);
      if (index != null) {
        ops.dropIndex(db, col, index);
        col.removeMetaIndexByName(indexName);
        snapshot.commit();
      }
      return index != null;
    }
  }

  ImmutableMetaSnapshot getMetaSnapshot() {
    return metadata.getSnapshot();
  }

  /**
   * Returns the database with the given name or creates one if there is none, in which case the
   * snapshot is commited.
   */
  private MutableMetaDatabase getDbOrCreate(
      TransactionalSnapshot snapshot, DdlOperationExecutor op, String dbName) {
    MutableMetaDatabase db = snapshot.getMetaDatabaseByName(dbName);
    if (db == null) {
      db = snapshot.addMetaDatabase(
          dbName,
          idFactory.toDatabaseIdentifier(snapshot, dbName)
      );
      op.addDatabase(db);
      snapshot.commit();
    }
    return db;
  }

  private MutableMetaDatabase getDbOrThrow(TransactionalSnapshot snapshot, String dbName)
      throws UnexistentDatabaseException {
    MutableMetaDatabase db = snapshot.getMetaDatabaseByName(dbName);
    if (db == null) {
      throw new UnexistentDatabaseException(dbName);
    }
    return db;
  }

  /**
   * Returns the collection with the given name or creates one if there is none, in which case the
   * snapshot is commited.
   */
  private MutableMetaCollection getColOrCreate(TransactionalSnapshot snapshot,
      DdlOperationExecutor ops, MutableMetaDatabase db,
      String colName) {
    MutableMetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      col = createCollectionPrivate(ops, snapshot, db, colName);
    }
    return col;
  }

  private MutableMetaCollection getColOrThrow(
      MutableMetaDatabase db, String colName) throws UnexistentCollectionException {
    MutableMetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      throw new UnexistentCollectionException(db.getName(), colName);
    }
    return col;
  }

  private String extractKeyName(AttributeReference.Key<?> key) {
    if (key instanceof AttributeReference.ObjectKey) {
      return ((AttributeReference.ObjectKey) key).getKey();
    } else {
      throw new IllegalArgumentException("Keys whose type is not object are not valid");
    }
  }

  private MutableMetaCollection createCollectionPrivate(
      DdlOperationExecutor ops,
      TransactionalSnapshot snapshot,
      MutableMetaDatabase db,
      String colName) {
    MutableMetaCollection col = db.addMetaCollection(colName,
        idFactory.toCollectionIdentifier(snapshot, db.getName(), colName));
    ops.addCollection(db, col);
    snapshot.commit();

    return col;
  }
}
