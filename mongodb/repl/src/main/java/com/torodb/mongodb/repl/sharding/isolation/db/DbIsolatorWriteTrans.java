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

package com.torodb.mongodb.repl.sharding.isolation.db;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.torod.WriteDocTransaction;
import com.torodb.torod.cursors.TorodCursor;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class DbIsolatorWriteTrans extends DbIsolatorTrans<WriteDocTransaction>
    implements WriteDocTransaction {

  public DbIsolatorWriteTrans(Converter converter, WriteDocTransaction decorated) {
    super(converter, decorated);
  }

  @Override
  public void insert(String dbName, String colName, Collection<KvDocument> documents) throws
      RollbackException, UserException {
    getDecorated().insert(getConverter().convertDatabaseName(dbName), colName, documents);
  }

  @Override
  public void insert(String dbName, String colName, KvDocument document) throws RollbackException,
      UserException {
    getDecorated().insert(getConverter().convertDatabaseName(dbName), colName, document);
  }

  @Override
  public void insert(String dbName, String colName, Stream<KvDocument> documents) throws
      RollbackException, UserException {
    getDecorated().insert(getConverter().convertDatabaseName(dbName), colName, documents);
  }

  @Override
  public void delete(String dbName, String colName, Cursor<Integer> cursor) {
    getDecorated().delete(getConverter().convertDatabaseName(dbName), colName, cursor);
  }

  @Override
  public void delete(String dbName, String colName, List<ToroDocument> candidates) {
    getDecorated().delete(getConverter().convertDatabaseName(dbName), colName, candidates);
  }

  @Override
  public void delete(String dbName, String colName, TorodCursor cursor) {
    getDecorated().delete(getConverter().convertDatabaseName(dbName), colName, cursor);
  }

  @Override
  public long deleteAll(String dbName, String colName) {
    return getDecorated().deleteAll(getConverter().convertDatabaseName(dbName), colName);
  }

  @Override
  public long deleteByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value) {
    return getDecorated().deleteByAttRef(getConverter().convertDatabaseName(dbName), colName,
        attRef, value);
  }

  @Override
  public void commit() throws RollbackException, UserException {
    getDecorated().commit();
  }

}
