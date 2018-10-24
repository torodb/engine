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

package com.torodb.mongodb.repl.sharding.isolation;

import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.exception.AlreadyExistentCollectionException;
import com.torodb.torod.exception.UnexistentCollectionException;
import com.torodb.torod.exception.UnexistentDatabaseException;
import com.torodb.torod.exception.UnsupportedIndexException;
import com.torodb.torod.exception.UserSchemaException;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 *
 */
public class SchemaOperationExecutorDecorator
    implements SchemaOperationExecutor {

  private final SchemaOperationExecutor decorated;

  public SchemaOperationExecutorDecorator(SchemaOperationExecutor decorated) {
    this.decorated = decorated;
  }

  public SchemaOperationExecutor getDecorated() {
    return decorated;
  }

  @Override
  public Stream<String> streamDbNames() {
    return decorated.streamDbNames();
  }

  @Override
  public boolean prepareSchema(String dbName, String colName, Collection<KvDocument> docs) throws
      UserSchemaException {
    return decorated.prepareSchema(dbName, colName, docs);
  }

  @Override
  public boolean prepareSchema(String dbName, String colName, KvDocument doc) throws
      UserSchemaException {
    return decorated.prepareSchema(dbName, colName, doc);
  }

  @Override
  public boolean prepareSchema(String dbName, String colName) throws UserSchemaException {
    return decorated.prepareSchema(dbName, colName);
  }

  @Override
  public void disableDataImportMode(String dbName) throws UnexistentDatabaseException {
    decorated.disableDataImportMode(dbName);
  }

  @Override
  public void enableDataImportMode(String dbName) throws UnexistentDatabaseException {
    decorated.enableDataImportMode(dbName);
  }

  @Override
  public void renameCollection(String fromDb, String fromCol, String toDb, String toCol) throws
      UnexistentDatabaseException, UnexistentCollectionException,
      AlreadyExistentCollectionException, UserException {
    decorated.renameCollection(fromDb, fromCol, toDb, toCol);
  }

  @Override
  public void createCollection(String dbName, String colName) throws RollbackException,
      UnexistentDatabaseException {
    decorated.createCollection(dbName, colName);
  }

  @Override
  public void dropCollection(String dbName, String colName) throws UnexistentDatabaseException {
    decorated.dropCollection(dbName, colName);
  }

  @Override
  public void createDatabase(String dbName) throws RollbackException, UserException {
    decorated.createDatabase(dbName);
  }

  @Override
  public void dropDatabase(String dbName) throws RollbackException {
    decorated.dropDatabase(dbName);
  }

  @Override
  public boolean createIndex(String dbName, String colName, String indexName,
      List<IndexFieldInfo> fields, boolean unique) throws UnexistentDatabaseException,
      UnexistentCollectionException, UnsupportedIndexException {
    return decorated.createIndex(dbName, colName, indexName, fields, unique);
  }

  @Override
  public boolean dropIndex(String dbName, String colName, String indexName) throws
      UnexistentDatabaseException, UnexistentCollectionException {
    return decorated.dropIndex(dbName, colName, indexName);
  }

  @Override
  public void close() {
    decorated.close();
  }

  @Override
  public boolean isClosed() {
    return decorated.isClosed();
  }

  @Override
  public Stream<IndexInfo> getIndexesInfo(String dbName, String colName) {
    return decorated.getIndexesInfo(dbName, colName);
  }
}