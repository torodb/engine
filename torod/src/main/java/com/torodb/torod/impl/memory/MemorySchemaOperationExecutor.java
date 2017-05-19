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

import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.exception.UserSchemaException;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 *
 */
class MemorySchemaOperationExecutor implements SchemaOperationExecutor {

  private final MemoryData.MdWriteTransaction trans;

  public MemorySchemaOperationExecutor(MemoryTorodServer server) {
    this.trans = server.getData().openWriteTransaction();
  }

  @Override
  public boolean prepareSchema(String dbName, String colName, Collection<KvDocument> docs) throws
      UserSchemaException {
    //import mode is ignored on this implementation
    return true;
  }

  @Override
  public void disableDataImportMode(String dbName) {
    //import mode is ignored on this implementation
  }

  @Override
  public void enableDataImportMode(String dbName) {
    //import mode is ignored on this implementation
  }

  @Override
  public void createDatabase(String db) throws RollbackException, UserException {
    trans.createDatabase(db);
    trans.commit();
  }

  @Override
  public void dropCollection(String dbName, String colName) throws RollbackException {
    trans.dropCollection(dbName, colName);
    trans.commit();
  }

  @Override
  public void renameCollection(String fromDb, String fromCollection, String toDb,
      String toCollection)
      throws RollbackException, UserException {
    trans.renameCollection(fromDb, fromCollection, toDb, toCollection);
    trans.commit();
  }

  @Override
  public void createCollection(String dbName, String colName) throws RollbackException {
    trans.createCollection(dbName, colName);
    trans.commit();
  }

  @Override
  public void dropDatabase(String dbName) throws RollbackException {
    trans.dropDatabase(dbName);
    trans.commit();
  }

  @Override
  public boolean createIndex(String dbName, String colName, String indexName,
      List<IndexFieldInfo> fields, boolean unique) {
    //Indexes are ignored on this mode
    return false;
  }

  @Override
  public boolean dropIndex(String dbName, String colName, String indexName) {
    //Indexes are ignored on this mode
    return false;
  }

  @Override
  public Stream<String> streamDbNames() {
    return trans.streamDbs();
  }

  @Override
  public Stream<IndexInfo> getIndexesInfo(String database, String collection) {
    return Stream.empty();
  }

  @Override
  public void close() {
    trans.close();
  }

  @Override
  public boolean isClosed() {
    return trans.isClosed();
  }
}