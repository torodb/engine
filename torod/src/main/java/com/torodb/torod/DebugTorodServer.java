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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.torodb.core.services.IdleTorodbService;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@link TorodServer} that registers transactions that are open.
 */
public class DebugTorodServer extends IdleTorodbService implements ProtectedServer {

  private static final Logger LOGGER = TorodLoggerFactory.get(DebugTorodServer.class);
  private final Cache<ElementId, ManagedElement> openTransactions = CacheBuilder.newBuilder()
      .weakValues()
      .build();
  private final TorodServer decorated;
  private Optional<SchemaOperationExecutor> activeSchemaHandler = Optional.empty();

  public DebugTorodServer(ThreadFactory threadFactory, TorodServer decorate) {
    super(threadFactory);
    this.decorated = decorate;
  }

  @Override
  protected void startUp() throws Exception {
    decorated.startAsync();
    decorated.awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    openTransactions.invalidateAll();
    
    decorated.stopAsync();
    decorated.awaitTerminated();
  }

  @Override
  public DocTransaction openReadTransaction(long timeout, TimeUnit unit)
      throws TimeoutException {
    DocTransaction inner = decorated.openReadTransaction(timeout, unit);
    DebugReadTransaction result = new DebugReadTransaction(inner);

    openTransactions.put(result.getElementId(), result);

    return result;
  }

  @Override
  public WriteDocTransaction openWriteTransaction(long timeout, TimeUnit unit)
      throws TimeoutException {
    WriteDocTransaction inner = decorated.openWriteTransaction(timeout, unit);
    DebugWriteTransaction result = new DebugWriteTransaction(inner);

    openTransactions.put(result.getElementId(), result);

    return result;
  }

  @Override
  public SchemaOperationExecutor openSchemaOperationExecutor(long timeout, TimeUnit unit)
      throws TimeoutException {
    SchemaOperationExecutor inner = decorated.openSchemaOperationExecutor(timeout, unit);
    DebugSchemaHandler result = new DebugSchemaHandler(inner);

    assert !activeSchemaHandler.isPresent();

    activeSchemaHandler = Optional.of(result);

    return result;
  }

  private class ElementId {}

  private interface ManagedElement extends AutoCloseable {

    ElementId getElementId();

    @Override
    public void close();

  }

  private class DebugReadTransaction extends DocTransactionDecorator<DocTransaction>
      implements ManagedElement {

    private final ElementId eid;
    private boolean closed = false;

    public DebugReadTransaction(DocTransaction trans) {
      super(trans);
      this.eid = new ElementId();
    }

    @Override
    public ElementId getElementId() {
      return eid;
    }

    @Override
    public synchronized void close() {
      if (!closed) {
        closed = true;
        getDecorated().close();
        openTransactions.invalidate(getElementId());
      }
    }

    @Override
    protected void finalize() throws Throwable {
      super.finalize();
      if (!closed) {
        LOGGER.warn("Read transaction finalized without being closed");
        close();
      }
    }
  }

  private class DebugWriteTransaction extends WriteDocTransactionDecorator
      implements ManagedElement {

    private final ElementId eid;
    private boolean closed = false;

    public DebugWriteTransaction(WriteDocTransaction trans) {
      super(trans);
      this.eid = new ElementId();
    }

    @Override
    public ElementId getElementId() {
      return eid;
    }

    @Override
    public synchronized void close() {
      if (!closed) {
        closed = true;
        openTransactions.invalidate(getElementId());
        getDecorated().close();
      }
    }

    @Override
    protected void finalize() throws Throwable {
      super.finalize();
      if (!closed) {
        LOGGER.warn("Write transaction finalized without being closed");
        close();
      }
    }

  }

  private class DebugSchemaHandler extends SchemaOperationExecutorDecorator {
    private boolean closed = false;

    public DebugSchemaHandler(SchemaOperationExecutor decorated) {
      super(decorated);
    }

    @Override
    public void close() {
      if (!getDecorated().isClosed()) {
        closed = true;
        activeSchemaHandler = Optional.empty();
        getDecorated().close();
      }
    }

    @Override
    protected void finalize() throws Throwable {
      super.finalize();
      if (!closed) {
        LOGGER.warn("Schema handler finalized without being closed");
        close();
      }
    }

  }

}