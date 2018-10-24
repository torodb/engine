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
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.exceptions.user.IndexNotFoundException;
import com.torodb.core.language.AttributeReference;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.mongodb.repl.sharding.isolation.TransDecorator;
import com.torodb.torod.CollectionInfo;
import com.torodb.torod.DocTransaction;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.cursors.TorodCursor;
import org.jooq.lambda.tuple.Tuple2;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DbIsolatorTrans<D extends DocTransaction> extends TransDecorator<D> {

  private final Converter converter;

  public DbIsolatorTrans(Converter converter, D decorated) {
    super(decorated);
    this.converter = converter;
  }

  Converter getConverter() {
    return converter;
  }

  @Override
  public IndexInfo getIndexInfo(String dbName, String colName, String idxName) throws
      IndexNotFoundException {
    return super.getIndexInfo(converter.convertDatabaseName(dbName), colName, idxName);
  }

  @Override
  public Stream<IndexInfo> getIndexesInfo(String dbName, String colName) {
    return super.getIndexesInfo(converter.convertDatabaseName(dbName), colName);
  }

  @Override
  public CollectionInfo getCollectionInfo(String dbName, String colName) throws
      CollectionNotFoundException {
    return super.getCollectionInfo(converter.convertDatabaseName(dbName), colName);
  }

  @Override
  public Stream<CollectionInfo> getCollectionsInfo(String dbName) {
    return super.getCollectionsInfo(converter.convertDatabaseName(dbName));
  }

  @Override
  public TorodCursor fetch(String dbName, String colName, Cursor<Integer> didCursor) {
    return super.fetch(converter.convertDatabaseName(dbName), colName, didCursor);
  }

  @Override
  public Cursor<Tuple2<Integer, KvValue<?>>> findByAttRefInProjection(String dbName, String colName,
      AttributeReference attRef,
      Collection<KvValue<?>> values) {
    return super.findByAttRefInProjection(converter.convertDatabaseName(dbName), colName,
        attRef, values);
  }

  @Override
  public TorodCursor findByAttRefIn(String dbName, String colName, AttributeReference attRef,
      Collection<KvValue<?>> values) {
    return super.findByAttRefIn(converter.convertDatabaseName(dbName), colName, attRef, values);
  }

  @Override
  public TorodCursor findByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value) {
    return super.findByAttRef(converter.convertDatabaseName(dbName), colName, attRef, value);
  }

  @Override
  public TorodCursor findAll(String dbName, String colName) {
    return super.findAll(converter.convertDatabaseName(dbName), colName);
  }

  @Override
  public long getDocumentsSize(String dbName, String colName) {
    return super.getDocumentsSize(converter.convertDatabaseName(dbName), colName);
  }

  @Override
  public long getCollectionSize(String dbName, String colName) {
    return super.getCollectionSize(converter.convertDatabaseName(dbName), colName);
  }

  @Override
  public long countAll(String dbName, String colName) {
    return super.countAll(converter.convertDatabaseName(dbName), colName);
  }

  @Override
  public long getDatabaseSize(String dbName) {
    return super.getDatabaseSize(converter.convertDatabaseName(dbName));
  }

  @Override
  public List<String> getDatabases() {
    return super.getDatabases().stream()
        .filter(converter::isVisibleDatabase)
        .collect(Collectors.toList());
  }

  @Override
  public boolean existsCollection(String dbName, String colName) {
    return super.existsCollection(converter.convertDatabaseName(dbName), colName);
  }

  @Override
  public boolean existsDatabase(String dbName) {
    return super.existsDatabase(converter.convertDatabaseName(dbName));
  }


}
