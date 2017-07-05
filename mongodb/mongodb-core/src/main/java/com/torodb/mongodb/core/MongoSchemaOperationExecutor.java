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

package com.torodb.mongodb.core;

import com.google.common.collect.ImmutableList;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.mongodb.utils.DefaultIdUtils;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.exception.AlreadyExistentCollectionException;
import com.torodb.torod.exception.UnexistentCollectionException;
import com.torodb.torod.exception.UnexistentDatabaseException;
import com.torodb.torod.exception.UnsupportedIndexException;
import com.torodb.torod.exception.UserSchemaException;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * A {@link SchemaOperationExecutor} decorator that adds the default _id index on newly created
 * collections
 */
class MongoSchemaOperationExecutor implements SchemaOperationExecutor {

  private static final ImmutableList<IndexFieldInfo> INDEXED_FIELD =
      ImmutableList.<IndexFieldInfo>of(new IndexFieldInfo(
          new AttributeReference(new AttributeReference.ObjectKey(DefaultIdUtils.ID_KEY)),
          FieldIndexOrdering.ASC.isAscending())
      );

  private final Logger logger;
  private final SchemaOperationExecutor delegate;
  private final Supervisor supervisor;

  public MongoSchemaOperationExecutor(Logger logger, SchemaOperationExecutor delegate,
      Supervisor supervisor) {
    this.logger = logger;
    this.delegate = delegate;
    this.supervisor = supervisor;
  }

  @Override
  public Stream<String> streamDbNames() {
    return delegate.streamDbNames();
  }

  private void createDefaultIndex(String dbName, String colName) {

    try {
      delegate.createIndex(dbName, colName, DefaultIdUtils.ID_INDEX, INDEXED_FIELD, true);
    } catch (UnsupportedIndexException ex) {
      supervisor.onError(this, ex);
    } catch (UnexistentCollectionException | UnexistentDatabaseException ex) {
      logger.debug("Trying to create the default _id index on unexistent namespace", ex);
    }
  }

  @Override
  public boolean prepareSchema(String dbName, String colName, Collection<KvDocument> docs) throws
      UserSchemaException {
    boolean result = delegate.prepareSchema(dbName, colName, docs);
    if (result) {
      createDefaultIndex(dbName, colName);
    }
    return result;
  }

  @Override
  public void disableDataImportMode(String dbName) throws UnexistentDatabaseException {
    delegate.disableDataImportMode(dbName);
  }

  @Override
  public void enableDataImportMode(String dbName) throws UnexistentDatabaseException {
    delegate.enableDataImportMode(dbName);
  }

  @Override
  public void renameCollection(String fromDb, String fromCol, String toDb, String toCol) throws
      UnexistentDatabaseException, UnexistentCollectionException, 
      AlreadyExistentCollectionException, UserException {
    delegate.renameCollection(fromDb, fromCol, toDb, toCol);
    createDefaultIndex(toDb, toCol);
  }

  @Override
  public void createCollection(String dbName, String colName) throws RollbackException,
      UnexistentDatabaseException {
    delegate.createCollection(dbName, colName);
    createDefaultIndex(dbName, colName);
  }

  @Override
  public void dropCollection(String dbName, String colName) throws UnexistentDatabaseException {
    delegate.dropCollection(dbName, colName);
  }

  @Override
  public void createDatabase(String dbName) throws RollbackException, UserException {
    delegate.createDatabase(dbName);
  }

  @Override
  public void dropDatabase(String dbName) throws RollbackException {
    delegate.dropDatabase(dbName);
  }

  @Override
  public boolean createIndex(String dbName, String colName, String indexName,
      List<IndexFieldInfo> fields, boolean unique) throws UnexistentDatabaseException,
      UnexistentCollectionException, UnsupportedIndexException {
    return delegate.createIndex(dbName, colName, indexName, fields, unique);
  }

  @Override
  public boolean dropIndex(String dbName, String colName, String indexName) throws
      UnexistentDatabaseException, UnexistentCollectionException {
    return delegate.dropIndex(dbName, colName, indexName);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public boolean isClosed() {
    return delegate.isClosed();
  }

  @Override
  public Stream<IndexInfo> getIndexesInfo(String dbName, String colName) {
    return delegate.getIndexesInfo(dbName, colName);
  }

}
