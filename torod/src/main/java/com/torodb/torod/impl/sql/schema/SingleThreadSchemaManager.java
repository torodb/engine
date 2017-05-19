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

package com.torodb.torod.impl.sql.schema;

import com.torodb.common.util.Empty;
import com.torodb.core.backend.DdlOperationExecutor;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.services.ExecutorTorodbService;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.torod.IndexFieldInfo;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

/**
 *
 */
@ThreadSafe
class SingleThreadSchemaManager extends ExecutorTorodbService<ExecutorService>
    implements SchemaManager {

  private final Logic logic;

  SingleThreadSchemaManager(ThreadFactory threadFactory,
      ConcurrentToolsFactory concurrentToolsFactory,
      Logic logic) {
    super(threadFactory, () -> concurrentToolsFactory.createExecutorServiceWithMaxThreads(
        "schema-manager", 1)
    );
    this.logic = logic;
  }

  /**
   * A simple constructor, usually used for testing propose.
   *
   * It uses a default thread factory and creates a executor service with a single thread.
   * @param logic
   */
  SingleThreadSchemaManager(Logic logic) {
    super(Thread::new, () -> new ThreadPoolExecutor(
        1, 1, 0, TimeUnit.DAYS, new LinkedBlockingQueue<>())
    );
    this.logic = logic;
  }

  @Override
  public CompletableFuture<Empty> refreshMetadata(DdlOperationExecutor ops) {
    return executeVoid(() -> logic.refreshMetadata(ops));
  }

  @Override
  public CompletableFuture<Boolean> prepareSchema(DdlOperationExecutor ops, String db,
      String collection, Collection<KvDocument> docs) {
    return execute(() -> logic.prepareSchema(ops, db, collection, docs));
  }

  @Override
  public CompletableFuture<Empty> createDatabase(DdlOperationExecutor ops, String dbName) {
    return executeVoid(() -> logic.createDatabase(ops, dbName));
  }

  @Override
  public CompletableFuture<Empty> dropDatabase(DdlOperationExecutor ops, String dbName) {
    return executeVoid(() -> logic.dropDatabase(ops, dbName));
  }

  @Override
  public CompletableFuture<Empty> createCollection(DdlOperationExecutor ops, String dbName,
      String colName) {
    return executeVoid(() -> logic.createCollection(ops, dbName, colName));
  }

  @Override
  public CompletableFuture<Empty> dropCollection(DdlOperationExecutor ops, String dbName,
      String colName) {
    return executeVoid(() -> logic.dropCollection(ops, dbName, colName));
  }

  @Override
  public CompletableFuture<Empty> renameCollection(DdlOperationExecutor ops, String fromDb,
      String fromCol, String toDb, String toCol) {
    return executeVoid(() -> logic.renameCollection(ops, fromDb, fromCol, toDb, toCol));
  }

  @Override
  public CompletableFuture<Empty> disableDataImportMode(DdlOperationExecutor ops, String dbName) {
    return executeVoid(() -> logic.disableDataImportMode(ops, dbName));
  }

  @Override
  public CompletableFuture<Empty> enableDataImportMode(DdlOperationExecutor ops, String dbName) {
    return executeVoid(() -> logic.enableDataImportMode(ops, dbName));
  }

  @Override
  public CompletableFuture<Boolean> createIndex(DdlOperationExecutor ops, String dbName,
      String colName, String indexName, List<IndexFieldInfo> fields, boolean unique) {
    return execute(() -> logic.createIndex(ops, dbName, colName, indexName, fields, unique));
  }

  @Override
  public CompletableFuture<Boolean> dropIndex(DdlOperationExecutor ops, String dbName, 
      String colName, String indexName) {
    return execute(() -> logic.dropIndex(ops, dbName, colName, indexName));
  }

  @Override
  public CompletableFuture<ImmutableMetaSnapshot> getMetaSnapshot() {
    return execute(() -> logic.getMetaSnapshot());
  }

  @Override
  public <E> CompletableFuture<E> executeAtomically(Function<ImmutableMetaSnapshot, E> fun) {
    return execute(() -> logic.executeAtomically(fun));
  }

  private CompletableFuture<Empty> executeVoid(Runnable supplier) {
    return execute(supplier)
        .thenApply((ignored) -> Empty.getInstance());
  }
}