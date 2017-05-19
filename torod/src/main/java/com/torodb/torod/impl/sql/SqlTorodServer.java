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

import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.backend.BackendService;
import com.torodb.core.backend.DdlOperationExecutor;
import com.torodb.core.backend.DmlTransaction;
import com.torodb.core.backend.WriteDmlTransaction;
import com.torodb.core.concurrent.CompletableFutureUtils;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.torod.DocTransaction;
import com.torodb.torod.ProtectedServer;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.TorodLoggerFactory;
import com.torodb.torod.WriteDocTransaction;
import com.torodb.torod.impl.sql.schema.SchemaManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

/**
 *
 */
public class SqlTorodServer extends IdleTorodbService implements ProtectedServer {

  private static final Logger LOGGER = TorodLoggerFactory.get(SqlTorodServer.class);
  private final CompletableFutureUtils completableFutureUtils;
  private final BackendService backend;
  private final SchemaManager schemaManager;
  private final SqlWriteTransaction.PrepareSchemaCallback prepareSchemaCallback =
      this::prepareSchema;
  private final ReadDocTransactionFactory readTransFactory;
  private final WriteDocTransactionFactory writeTransFactory;
  private final ReservedIdGenerator idGenerator;

  @Inject
  public SqlTorodServer(@TorodbIdleService ThreadFactory threadFactory,
      CompletableFutureUtils completableFutureUtils,
      BackendService backend,
      SchemaManager schemaManager,
      ReadDocTransactionFactory readTransFactory,
      WriteDocTransactionFactory writeTransFactory,
      ReservedIdGenerator idGenerator) {
    super(threadFactory);
    this.completableFutureUtils = completableFutureUtils;
    this.backend = backend;
    this.schemaManager = schemaManager;
    this.readTransFactory = readTransFactory;
    this.writeTransFactory = writeTransFactory;
    this.idGenerator = idGenerator;
  }

  @Override
  public DocTransaction openReadTransaction(long timeout, TimeUnit unit)
      throws TimeoutException {
    return completableFutureUtils.executeOrTimeout(
        schemaManager.executeAtomically((snapshot) ->
            readTransFactory.createReadTransaction(backend.openReadTransaction(), snapshot)
        ),
        timeout,
        unit
    );
  }

  @Override
  public WriteDocTransaction openWriteTransaction(long timeout, TimeUnit unit)
      throws TimeoutException {
    return completableFutureUtils.executeOrTimeout(
        schemaManager.executeAtomically((snapshot) ->
            writeTransFactory.createWriteTransaction(
                backend.openWriteTransaction(),
                snapshot,
                prepareSchemaCallback
            )
        ),
        timeout,
        unit
    );
  }

  @Override
  public SchemaOperationExecutor openSchemaOperationExecutor(long timeout, TimeUnit unit)
      throws TimeoutException {
    return completableFutureUtils.executeOrTimeout(
        schemaManager.executeAtomically((snapshot) ->
            new SqlSchemaOperationExecutor(schemaManager, backend.openDdlOperationExecutor())),
        timeout,
        unit
    );
  }

  @Override
  protected void startUp() throws Exception {
    if (!backend.isRunning()) {
      LOGGER.debug("Waiting until backend layer is running");
      backend.awaitRunning();
    }

    schemaManager.start();
    schemaManager.awaitRunning();
    try (DdlOperationExecutor opsExec = backend.openDdlOperationExecutor()) {
      schemaManager.refreshMetadata(opsExec).join();
    }

    LOGGER.debug("Reading last used rids...");
    ImmutableMetaSnapshot snapshot = schemaManager.getMetaSnapshot().join();
    idGenerator.load(snapshot);
  }

  @Override
  protected void shutDown() throws Exception {
    schemaManager.stopAsync();
    schemaManager.awaitTerminated();
  }

  private void prepareSchema(String dbName, String colName, Collection<KvDocument> docs) {
    try (DdlOperationExecutor ddlOpsEx = backend.openDdlOperationExecutor()) {
      schemaManager.prepareSchema(ddlOpsEx, dbName, colName, docs);
    }
  }

  static interface ReadDocTransactionFactory {
    DocTransaction createReadTransaction(DmlTransaction dmlTrans, ImmutableMetaSnapshot snapshot);
  }

  static interface WriteDocTransactionFactory {
    WriteDocTransaction createWriteTransaction(WriteDmlTransaction dmlTrans,
        ImmutableMetaSnapshot snapshot,
        SqlWriteTransaction.PrepareSchemaCallback prepareSchemaCallback);
  }

}
