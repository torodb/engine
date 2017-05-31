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

import com.google.common.base.Preconditions;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.backend.WriteDmlTransaction;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.torod.TorodLoggerFactory;
import com.torodb.torod.WriteDocTransaction;
import org.apache.logging.log4j.Logger;

import java.util.Collection;

class SqlWriteTransaction extends SqlTransaction<WriteDmlTransaction>
    implements WriteDocTransaction {

  private static final Logger LOGGER = TorodLoggerFactory.get(SqlWriteTransaction.class);

  private final InsertD2RTranslator insertAnalyzer;
  private final PrepareSchemaCallback prepareSchemaCallback;

  public SqlWriteTransaction(WriteDmlTransaction backendTrans,
      ImmutableMetaSnapshot snapshot, R2DTranslator r2dTrans, TableRefFactory tableRefFactory,
      InsertD2RTranslator insertAnalyzer, PrepareSchemaCallback prepareSchemaCallback) {
    super(backendTrans, snapshot, r2dTrans, tableRefFactory);
    this.insertAnalyzer = insertAnalyzer;
    this.prepareSchemaCallback = prepareSchemaCallback;
  }


  @Override
  public void insert(String dbName, String colName, Collection<KvDocument> documents) throws
      RollbackException, UserException {
    Preconditions.checkState(!isClosed());

    try {
      CollectionData data = insertAnalyzer.analyze(
          getMetaSnapshot(), dbName, colName, documents.stream());
      
      MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
      assert db != null;
      MetaCollection col = db.getMetaCollectionByName(colName);

      for (DocPartData docPartData : data) {
        getBackendTransaction().insert(db, col, docPartData);
      }
    } catch (IncompatibleSchemaException ex) {
      LOGGER.debug("Documents don't fit on {}.{}. Rolling back", dbName, colName);
      LOGGER.trace("Exception:", ex);
      this.close();

      prepareSchemaCallback.prepareSchema(dbName, colName, documents);

      throw new RollbackException(ex);
    }
  }

  @Override
  public void delete(String dbName, String colName, Cursor<Integer> cursor) {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return;
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return;
    }

    getBackendTransaction().deleteDids(db, col, cursor.getRemaining());
  }

  @Override
  public long deleteAll(String dbName, String colName) {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return 0;
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return 0;
    }

    Collection<Integer> dids = getBackendTransaction()
        .findAll(db, col)
        .asDidCursor()
        .getRemaining();
    getBackendTransaction().deleteDids(db, col, dids);

    return dids.size();
  }

  @Override
  public long deleteByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value) {
    MetaDatabase db = getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return 0;
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return 0;
    }

    TableRef tableRef = extractTableRef(attRef);
    String lastKey = extractKeyName(attRef.getKeys().get(attRef.getKeys().size() - 1));

    MetaDocPart docPart = col.getMetaDocPartByTableRef(tableRef);
    if (docPart == null) {
      return 0;
    }

    MetaField field = docPart.getMetaFieldByNameAndType(lastKey, FieldType.from(value.getType()));
    if (field == null) {
      return 0;
    }

    Collection<Integer> dids = getBackendTransaction()
        .findByField(db, col, docPart, field, value)
        .asDidCursor()
        .getRemaining();
    getBackendTransaction().deleteDids(db, col, dids);

    return dids.size();
  }

  @Override
  public void commit() throws RollbackException, UserException {
    getBackendTransaction().commit();
  }

  @FunctionalInterface
  public static interface PrepareSchemaCallback {

    /**
     * An asynchronous request to update the schema so that:
     * <ol>
     * <li>There is a database with the given name</li>
     * <li>That database contains a collection with the given name</li>
     * <li>All documents <em>fit</em> on that collection</li>
     * </ol>
     *
     * @param dbName  The name of the database
     * @param colName The name of the collection
     * @param docs    The documents that must fit on the collection
     */
    void prepareSchema(String dbName, String colName, Collection<KvDocument> docs);
  }
}