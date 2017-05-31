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

import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.mongodb.repl.sharding.isolation.SchemaOperationExecutorDecorator;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.exception.AlreadyExistentCollectionException;
import com.torodb.torod.exception.UnexistentCollectionException;
import com.torodb.torod.exception.UnexistentDatabaseException;
import com.torodb.torod.exception.UserSchemaException;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 *
 */
public class DbIsolatorSchemaOperationExecutor 
    extends SchemaOperationExecutorDecorator {

  private final Converter converter; 

  public DbIsolatorSchemaOperationExecutor(Converter converter, SchemaOperationExecutor decorated) {
    super(decorated);
    this.converter = converter;
  }

  @Override
  public Stream<IndexInfo> getIndexesInfo(String dbName, String colName) {
    return getDecorated().getIndexesInfo(converter.convertDatabaseName(dbName), colName);
  }

  @Override
  public boolean dropIndex(String dbName, String colName, String indexName) throws
      UnexistentDatabaseException, UnexistentCollectionException {
    return getDecorated().dropIndex(
        converter.convertDatabaseName(dbName),
        colName,
        converter.convertIndexName(indexName)
    );
  }

  @Override
  public boolean createIndex(String dbName, String colName, String indexName,
      List<IndexFieldInfo> fields, boolean unique) throws UnexistentDatabaseException,
      UnexistentCollectionException, UserException {
    return getDecorated().createIndex(
        converter.convertDatabaseName(dbName),
        colName,
        converter.convertIndexName(indexName),
        fields,
        unique
    );
  }

  @Override
  public void dropDatabase(String dbName) throws RollbackException {
    getDecorated().dropDatabase(converter.convertDatabaseName(dbName));
  }

  @Override
  public void createDatabase(String dbName) throws RollbackException, UserException {
    getDecorated().createDatabase(converter.convertDatabaseName(dbName));
  }

  @Override
  public void dropCollection(String dbName, String colName) throws UnexistentDatabaseException {
    getDecorated().dropCollection(converter.convertDatabaseName(dbName), colName);
  }

  @Override
  public void createCollection(String dbName, String colName) throws RollbackException,
      UnexistentDatabaseException {
    getDecorated().createCollection(converter.convertDatabaseName(dbName), colName);
  }

  @Override
  public void renameCollection(String fromDb, String fromCol, String toDb, String toCol) throws
      UnexistentDatabaseException, UnexistentCollectionException, 
      AlreadyExistentCollectionException, UserException {
    getDecorated().renameCollection(
        converter.convertDatabaseName(toDb),
        fromCol,
        converter.convertDatabaseName(toDb),
        toCol
    );
  }

  @Override
  public void enableDataImportMode(String dbName) throws UnexistentDatabaseException {
    getDecorated().enableDataImportMode(converter.convertDatabaseName(dbName));
  }

  @Override
  public void disableDataImportMode(String dbName) throws UnexistentDatabaseException {
    getDecorated().disableDataImportMode(converter.convertDatabaseName(dbName));
  }

  @Override
  public boolean prepareSchema(String dbName, String colName) throws UserSchemaException {
    return getDecorated().prepareSchema(converter.convertDatabaseName(dbName), colName);
  }

  @Override
  public boolean prepareSchema(String dbName, String colName, KvDocument doc) throws
      UserSchemaException {
    return getDecorated().prepareSchema(converter.convertDatabaseName(dbName), colName, doc);
  }

  @Override
  public boolean prepareSchema(String dbName, String colName, Collection<KvDocument> docs) throws
      UserSchemaException {
    return getDecorated().prepareSchema(converter.convertDatabaseName(dbName), colName, docs);
  }

  @Override
  public Stream<String> streamDbNames() {
    return getDecorated().streamDbNames()
        .filter(converter::isVisibleDatabase)
        .map(converter::unconvertDatabaseName);
  }

}
