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

package com.torodb.torod;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.cursors.IteratorCursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.torod.cursors.TorodCursor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public interface WriteDocTransaction extends DocTransaction {

  /**
   * Insert a batch of documents on a collection.
   *
   * <p>If the indicated database (or collection) does not exist, then it will be created. In that
   * case, if the transaction is rollbacked, it is not defined if the database (or collection)
   * creation will rollbacked or not.
   *
   * @param dbName the database where documents will be inserted.
   * @param colName the collection where documents will be inserted.
   * @param documents the documents that will be inserted
   */
  public void insert(String dbName, String colName, Collection<KvDocument> documents) throws
      RollbackException, UserException;

  /**
   * Like {@link #insert(java.lang.String, java.lang.String, java.util.Collection)}, but accepts a
   * a single document instead of a collection of them.
   */
  public default void insert(String dbName, String colName, KvDocument document) throws
      RollbackException, UserException {
    insert(dbName, colName, Collections.singleton(document));
  }

  /**
   * Like {@link #insert(java.lang.String, java.lang.String, java.util.Collection)}, but accepts a
   * stream of documents instead of a collection of them.
   */
  public default void insert(String dbName, String colName, Stream<KvDocument> documents) throws
      RollbackException, UserException {
    insert(dbName, colName, documents.collect(Collectors.toList()));
  }

  public default void delete(String dbName, String colName, List<ToroDocument> candidates) {
    delete(dbName, colName, new IteratorCursor<>(candidates.stream().map(ToroDocument::getId)
        .iterator()));
  }

  public default void delete(String dbName, String colName, TorodCursor cursor) {
    delete(dbName, colName, cursor.asDidCursor());
  }

  public void delete(String dbName, String colName, Cursor<Integer> cursor);

  public long deleteAll(String dbName, String colName);

  public long deleteByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value);

  public void commit() throws RollbackException, UserException;

}