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

package com.torodb.torod.impl.sql;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.backend.BackendCursor;
import com.torodb.core.backend.DmlTransaction;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.cursors.EmptyCursor;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.exceptions.user.IndexNotFoundException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Key;
import com.torodb.core.language.AttributeReference.ObjectKey;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.torod.CollectionInfo;
import com.torodb.torod.CollectionInfo.Type;
import com.torodb.torod.DocTransaction;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.TorodLoggerFactory;
import com.torodb.torod.cursors.EmptyTorodCursor;
import com.torodb.torod.cursors.TorodCursor;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.tuple.Tuple2;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.json.Json;

class SqlTransaction<BT extends DmlTransaction> implements DocTransaction {

  private static final Logger LOGGER = TorodLoggerFactory.get(SqlTransaction.class);
  private final BT backendTrans;
  private final ImmutableMetaSnapshot snapshot;
  private final R2DTranslator r2d;
  private final TableRefFactory tableRefFactory;

  public SqlTransaction(BT backendTrans, ImmutableMetaSnapshot snapshot, R2DTranslator r2d,
      TableRefFactory tableRefFactory) {
    this.backendTrans = backendTrans;
    this.snapshot = snapshot;
    this.r2d = r2d;
    this.tableRefFactory = tableRefFactory;
  }

  protected BT getBackendTransaction() {
    return backendTrans;
  }

  protected ImmutableMetaSnapshot getMetaSnapshot() {
    return snapshot;
  }

  @Override
  public boolean existsDatabase(String dbName) {
    MetaDatabase metaDb = getMetaSnapshot().getMetaDatabaseByName(dbName);
    return metaDb != null;
  }

  @Override
  public boolean existsCollection(String dbName, String colName) {
    MetaDatabase metaDb = getMetaSnapshot().getMetaDatabaseByName(dbName);
    return metaDb != null && metaDb.getMetaCollectionByName(colName) != null;
  }

  @Override
  public List<String> getDatabases() {
    return getMetaSnapshot().streamMetaDatabases()
        .map(metaDb -> metaDb.getName())
        .collect(Collectors.toList());
  }

  @Override
  public long getDatabaseSize(String dbName) {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return 0L;
    }
    return getBackendTransaction().getDatabaseSize(db);
  }

  @Override
  public long countAll(String dbName, String colName) {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return 0;
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return 0;
    }
    return getBackendTransaction().countAll(db, col);
  }

  @Override
  public long getCollectionSize(String dbName, String colName) {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return 0;
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return 0;
    }
    return getBackendTransaction().getCollectionSize(db, col);
  }

  @Override
  public long getDocumentsSize(String dbName, String colName) {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return 0;
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return 0;
    }
    return getBackendTransaction().getDocumentsSize(db, col);
  }

  @Override
  public TorodCursor findAll(String dbName, String colName) {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      LOGGER.trace("Collection " + dbName + '.' + colName
          + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    return toToroCursor(getBackendTransaction().findAll(db, col));
  }

  @Override
  public TorodCursor findByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value) {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      LOGGER.trace("Collection " + dbName + '.' + colName
          + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    TableRef ref = extractTableRef(attRef);
    String lastKey = extractKeyName(attRef.getKeys().get(attRef.getKeys().size() - 1));

    MetaDocPart docPart = col.getMetaDocPartByTableRef(ref);
    if (docPart == null) {
      LOGGER.trace("DocPart " + dbName + '.' + colName + '.' + ref
          + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }

    MetaField field = docPart.getMetaFieldByNameAndType(lastKey, FieldType.from(value.getType()));
    if (field == null) {
      LOGGER.trace("Field " + dbName + '.' + colName + '.' + ref + '.' + lastKey
          + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }

    return toToroCursor(getBackendTransaction()
        .findByField(db, col, docPart, field, value)
    );
  }

  @Override
  public TorodCursor findByAttRefIn(String dbName, String colName, AttributeReference attRef,
      Collection<KvValue<?>> values) {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      LOGGER.trace("Collection " + dbName + '.' + colName
          + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    if (values.isEmpty()) {
      LOGGER.trace(
          "An empty list of values have been given as in condition. An empty cursor is returned");
      return new EmptyTorodCursor();
    }

    TableRef ref = extractTableRef(attRef);
    String lastKey = extractKeyName(attRef.getKeys().get(attRef.getKeys().size() - 1));

    MetaDocPart docPart = col.getMetaDocPartByTableRef(ref);
    if (docPart == null) {
      LOGGER.trace("DocPart " + dbName + '.' + colName + '.' + ref
          + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }

    Multimap<MetaField, KvValue<?>> valuesMap = ArrayListMultimap.create();
    for (KvValue<?> value : values) {
      MetaField field = docPart.getMetaFieldByNameAndType(lastKey, FieldType.from(value.getType()));
      if (field != null) {
        valuesMap.put(field, value);
      }
    }
    return toToroCursor(getBackendTransaction()
        .findByFieldIn(db, col, docPart, valuesMap)
    );
  }

  @Override
  public Cursor<Tuple2<Integer, KvValue<?>>> findByAttRefInProjection(String dbName,
      String colName, AttributeReference attRef, Collection<KvValue<?>> values) {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
      return new EmptyCursor<>();
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      LOGGER.trace("Collection " + dbName + '.' + colName
          + " does not exist. An empty cursor is returned");
      return new EmptyCursor<>();
    }
    if (values.isEmpty()) {
      LOGGER.trace(
          "An empty list of values have been given as in condition. An empty cursor is returned");
      return new EmptyCursor<>();
    }

    TableRef ref = extractTableRef(attRef);
    String lastKey = extractKeyName(attRef.getKeys().get(attRef.getKeys().size() - 1));

    MetaDocPart docPart = col.getMetaDocPartByTableRef(ref);
    if (docPart == null) {
      LOGGER.trace("DocPart " + dbName + '.' + colName + '.' + ref
          + " does not exist. An empty cursor is returned");
      return new EmptyCursor<>();
    }

    Multimap<MetaField, KvValue<?>> valuesMap = ArrayListMultimap.create();
    for (KvValue<?> value : values) {
      MetaField field = docPart.getMetaFieldByNameAndType(lastKey, FieldType.from(value.getType()));
      if (field != null) {
        valuesMap.put(field, value);
      }
    }
    return getBackendTransaction()
        .findByFieldInProjection(db, col, docPart, valuesMap);
  }

  @Override
  public TorodCursor fetch(String dbName, String colName, Cursor<Integer> didCursor) {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      LOGGER.trace("Collection " + dbName + '.' + colName
          + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    return toToroCursor(getBackendTransaction()
        .fetch(db, col, didCursor)
    );
  }

  private TorodCursor toToroCursor(BackendCursor backendCursor) {
    return new LazyTorodCursor(r2d, backendCursor);
  }

  @Override
  public Stream<CollectionInfo> getCollectionsInfo(String dbName) {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return Stream.empty();
    }

    return db.streamMetaCollections().map(metaCol -> new CollectionInfo(metaCol.getName(),
        Type.COLLECTION, Json.createObjectBuilder().build()));
  }

  @Override
  public CollectionInfo getCollectionInfo(String dbName, String colName) throws
      CollectionNotFoundException {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      throw new CollectionNotFoundException(dbName, colName);
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      throw new CollectionNotFoundException(dbName, colName);
    }

    return new CollectionInfo(db.getMetaCollectionByName(colName).getName(), Type.COLLECTION, Json
        .createObjectBuilder().build());
  }

  @Override
  public Stream<IndexInfo> getIndexesInfo(String dbName, String colName) {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return Stream.empty();
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return Stream.empty();
    }

    return col.streamContainedMetaIndexes()
        .map(metaIdx -> createIndexInfo(metaIdx));
  }

  @Override
  public IndexInfo getIndexInfo(String dbName, String colName, String idxName) throws
      IndexNotFoundException {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      throw new IndexNotFoundException(dbName, colName, idxName);
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      throw new IndexNotFoundException(dbName, colName, idxName);
    }
    MetaIndex idx = col.getMetaIndexByName(idxName);
    if (idx == null) {
      throw new IndexNotFoundException(dbName, colName, idxName);
    }

    return createIndexInfo(idx);
  }

  protected IndexInfo createIndexInfo(MetaIndex metaIndex) {
    IndexInfo.Builder indexInfoBuilder = new IndexInfo.Builder(metaIndex.getName(), metaIndex
        .isUnique());

    metaIndex.iteratorFields()
        .forEachRemaining(metaIndexField ->
            indexInfoBuilder.addField(
                getAttrivuteReference(metaIndexField.getTableRef(), metaIndexField.getFieldName()),
                metaIndexField.getOrdering().isAscending()));

    return indexInfoBuilder.build();
  }

  protected AttributeReference getAttrivuteReference(TableRef tableRef, String name) {
    AttributeReference.Builder attributeReferenceBuilder = new AttributeReference.Builder();

    while (!tableRef.isRoot()) {
      attributeReferenceBuilder.addObjectKeyAsFirst(tableRef.getName());
      tableRef = tableRef.getParent().get();
    }

    attributeReferenceBuilder.addObjectKey(name);

    return attributeReferenceBuilder.build();
  }

  protected TableRef extractTableRef(AttributeReference attRef) {
    return tableRefFactory.translate(attRef);
  }

  protected String extractKeyName(Key<?> key) {
    if (key instanceof ObjectKey) {
      return ((ObjectKey) key).getKey();
    } else {
      throw new IllegalArgumentException("Keys whose type is not object are not valid");
    }
  }

  @Override
  public void rollback() {
    getBackendTransaction().rollback();
  }

  @Override
  public boolean isClosed() {
    return backendTrans.isClosed();
  }

  @Override
  public void close() {
    backendTrans.close();
  }

}
