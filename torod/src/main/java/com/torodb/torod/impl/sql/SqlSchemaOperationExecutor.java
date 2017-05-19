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

import com.torodb.core.TableRef;
import com.torodb.core.backend.DdlOperationExecutor;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.exception.AlreadyExistentCollectionException;
import com.torodb.torod.exception.UnexistentCollectionException;
import com.torodb.torod.exception.UnexistentDatabaseException;
import com.torodb.torod.impl.sql.schema.SchemaManager;
import com.torodb.torod.impl.sql.schema.SyncSchemaManager;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class SqlSchemaOperationExecutor implements SchemaOperationExecutor {

  private final SyncSchemaManager manager;
  private final DdlOperationExecutor ddlExecutor;

  public SqlSchemaOperationExecutor(SchemaManager manager, DdlOperationExecutor ddlExecutor) {
    this.manager = new SyncSchemaManager(manager);
    this.ddlExecutor = ddlExecutor;
  }

  @Override
  public boolean prepareSchema(String dbName, String colName, Collection<KvDocument> docs) {
    return manager.prepareSchema(ddlExecutor, dbName, colName, docs);
  }

  void refreshMetadata(DdlOperationExecutor ops) {
    manager.refreshMetadata(ops);
  }

  @Override
  public void createDatabase(String dbName) {
    manager.createDatabase(ddlExecutor, dbName);
  }

  @Override
  public void dropDatabase(String dbName) {
    manager.dropDatabase(ddlExecutor, dbName);
  }

  @Override
  public void createCollection(String dbName, String colName) throws RollbackException,
      UnexistentDatabaseException {
    manager.createCollection(ddlExecutor, dbName, colName);
  }

  @Override
  public void dropCollection(String dbName, String colName) throws UnexistentDatabaseException {
    manager.dropCollection(ddlExecutor, dbName, colName);
  }

  @Override
  public void renameCollection(String fromDb, String fromCol, String toDb,
      String toCol) throws UnexistentDatabaseException, UnexistentCollectionException,
      AlreadyExistentCollectionException {
    manager.renameCollection(ddlExecutor, fromDb, fromCol, toDb, toCol);
  }

  @Override
  public void disableDataImportMode(String dbName) throws
      UnexistentDatabaseException {
    manager.disableDataImportMode(ddlExecutor, dbName);
  }

  @Override
  public void enableDataImportMode(String dbName) throws
      UnexistentDatabaseException {
    manager.enableDataImportMode(ddlExecutor, dbName);
  }

  @Override
  public boolean createIndex(String dbName, String colName, String indexName,
      List<IndexFieldInfo> fields, boolean unique) throws UnexistentDatabaseException,
      UnexistentCollectionException {
    return manager.createIndex(ddlExecutor, dbName, colName, indexName, fields, unique);
  }

  @Override
  public boolean dropIndex(String dbName, String colName, String indexName)
      throws UnexistentDatabaseException, UnexistentCollectionException {
    return manager.dropIndex(ddlExecutor, dbName, colName, indexName);
  }

  @Override
  public Stream<IndexInfo> getIndexesInfo(String dbName, String colName) {
    return manager.executeAtomically(snapshot -> createIndexesInfo(snapshot, dbName, colName))
        .stream();
  }

  @Override
  public Stream<String> streamDbNames() {
    return manager.getMetaSnapshot().streamMetaDatabases()
        .map(MetaDatabase::getName);
  }

  @Override
  public boolean isClosed() {
    return ddlExecutor.isClosed();
  }

  @Override
  public void close() {
    ddlExecutor.close();
  }

  private List<IndexInfo> createIndexesInfo(MetaSnapshot snapshot, String dbName, String colName) {
    MetaDatabase db = snapshot.getMetaDatabaseByName(dbName);
    if (db == null) {
      return Collections.emptyList();
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return Collections.emptyList();
    }
    return col.streamContainedMetaIndexes()
        .map(this::createIndexInfo)
        .collect(Collectors.toList());
  }

  @SuppressWarnings("checkstyle:indentation")
  private IndexInfo createIndexInfo(MetaIndex metaIndex) {
    IndexInfo.Builder indexInfoBuilder = new IndexInfo.Builder(metaIndex.getName(), metaIndex
        .isUnique());

    metaIndex.iteratorFields()
        .forEachRemaining(metaIndexField ->
            indexInfoBuilder.addField(
                getAttributeReference(metaIndexField.getTableRef(), metaIndexField.getFieldName()),
                metaIndexField.getOrdering().isAscending()
            )
        );

    return indexInfoBuilder.build();
  }

  protected AttributeReference getAttributeReference(TableRef tableRef, String name) {
    AttributeReference.Builder attributeReferenceBuilder = new AttributeReference.Builder();

    while (!tableRef.isRoot()) {
      attributeReferenceBuilder.addObjectKeyAsFirst(tableRef.getName());
      tableRef = tableRef.getParent().get();
    }

    attributeReferenceBuilder.addObjectKey(name);

    return attributeReferenceBuilder.build();
  }

}