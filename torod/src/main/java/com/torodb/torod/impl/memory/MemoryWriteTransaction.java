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

package com.torodb.torod.impl.memory;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.torod.WriteDocTransaction;
import com.torodb.torod.impl.memory.MemoryData.MdTransaction;

import java.util.Collection;


/**
 *
 */
class MemoryWriteTransaction extends MemoryTransaction implements WriteDocTransaction {

  private final MemoryData.MdWriteTransaction trans;

  public MemoryWriteTransaction(MemoryTorodServer server) {
    this.trans = server.getData().openWriteTransaction();
  }

  @Override
  protected MdTransaction getTransaction() {
    return trans;
  }

  @Override
  public void insert(String db, String collection, Collection<KvDocument> documents) throws
      RollbackException, UserException {
    trans.insert(db, collection, documents.stream());
  }

  @Override
  public long deleteAll(String dbName, String colName) {
    long count = trans.streamCollection(dbName, colName).count();
    trans.deleteAll(dbName, colName);
    return count;
  }

  @Override
  public long deleteByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value) {
    return trans.delete(dbName, colName, streamByAttRef(dbName, colName, attRef, value).map(
        ToroDocument::getId));
  }

  @Override
  public void delete(String dbName, String colName, Cursor<Integer> cursor) {
    trans.delete(dbName, colName, cursor.getRemaining().stream());
  }

  @Override
  public void rollback() {
    trans.rollback();
  }

  @Override
  public void commit() throws RollbackException, UserException {
    trans.commit();
  }

}
