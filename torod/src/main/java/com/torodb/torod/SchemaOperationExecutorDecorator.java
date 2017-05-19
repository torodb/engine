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

import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.values.KvDocument;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 *
 */
public class SchemaOperationExecutorDecorator implements SchemaOperationExecutor {

  private final SchemaOperationExecutor decorated;

  public SchemaOperationExecutorDecorator(SchemaOperationExecutor decorated) {
    this.decorated = decorated;
  }

  protected SchemaOperationExecutor getDecorated() {
    return decorated;
  }

  @Override
  public boolean prepareSchema(String dbName, String colName, Collection<KvDocument> docs)  {
    return decorated.prepareSchema(dbName, colName, docs);
  }

  @Override
  public void disableDataImportMode(String dbName) {
    decorated.disableDataImportMode(dbName);
  }

  @Override
  public void enableDataImportMode(String dbName) {
    decorated.enableDataImportMode(dbName);
  }

  @Override
  public void createCollection(String dbName, String colName) throws RollbackException {
    decorated.createCollection(dbName, colName);
  }
  
  @Override
  public void renameCollection(String fromDb, String fromCollection, String toDb,
      String toCollection) throws RollbackException, UserException {
    decorated.renameCollection(fromDb, fromCollection, toDb, toCollection);
  }

  @Override
  public void dropCollection(String db, String collection) throws RollbackException {
    decorated.dropCollection(db, collection);
  }

  @Override
  public void createDatabase(String db) throws RollbackException, UserException {
    decorated.createDatabase(db);
  }

  @Override
  public void dropDatabase(String db) throws RollbackException {
    decorated.dropDatabase(db);
  }

  @Override
  public boolean createIndex(String dbName, String colName, String indexName,
      List<IndexFieldInfo> fields, boolean unique) throws UserException {
    return decorated.createIndex(dbName, colName, indexName, fields, unique);
  }

  @Override
  public boolean dropIndex(String dbName, String colName, String indexName) {
    return decorated.dropIndex(dbName, colName, indexName);
  }

  @Override
  public Stream<String> streamDbNames() {
    return decorated.streamDbNames();
  }

  @Override
  public Stream<IndexInfo> getIndexesInfo(String database, String collection) {
    return decorated.getIndexesInfo(database, collection);
  }

  @Override
  public void close() {
    decorated.close();
  }

  @Override
  public boolean isClosed() {
    return decorated.isClosed();
  }
}