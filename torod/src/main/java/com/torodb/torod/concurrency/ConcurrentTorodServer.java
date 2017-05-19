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

package com.torodb.torod.concurrency;

import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.torod.DefaultTimeoutHandler;
import com.torodb.torod.DocTransaction;
import com.torodb.torod.DocTransactionDecorator;
import com.torodb.torod.ProtectedServer;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.SchemaOperationExecutorDecorator;
import com.torodb.torod.TorodLoggerFactory;
import com.torodb.torod.TorodServer;
import com.torodb.torod.WriteDocTransaction;
import com.torodb.torod.WriteDocTransactionDecorator;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

/**
 *
 */
public class ConcurrentTorodServer extends IdleTorodbService implements TorodServer {
  private final ConcurrentServerGuard guard;
  private final ProtectedServer decorated;
  private static final Logger LOGGER = TorodLoggerFactory.get(ConcurrentTorodServer.class);
  private final DefaultTimeoutHandler timeoutHandler = new DefaultTimeoutHandler(
      this,
      Duration.ofSeconds(30)
  );

  @Inject
  public ConcurrentTorodServer(@TorodbIdleService ThreadFactory threadFactory,
      ProtectedServer decorated, ConcurrentServerGuard guard) {
    super(threadFactory);
    this.decorated = decorated;
    this.guard = guard;
  }

  @Override
  protected void startUp() throws Exception {
    decorated.startAsync();
    decorated.awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    decorated.stopAsync();
    decorated.awaitTerminated();
  }

  @Override
  public DocTransaction openReadTransaction(long timeout, TimeUnit unit)
      throws TimeoutException {
    checkRunning();
    DocTransaction decoratedTrans = decorated.openReadTransaction(timeout, unit);
    boolean correct = false;
    try {
      DocTransaction result = guard.createTransaction(
          timeout,
          unit,
          () -> new ManagedReadTransaction(decoratedTrans)
      );
      correct = true;

      return result;
    } finally {
      if (!correct) {
        decoratedTrans.close();
      }
    }
  }

  @Override
  public DocTransaction openReadTransaction() throws TimeoutException {
    checkRunning();
    return timeoutHandler.openReadTransaction();
  }

  @Override
  public WriteDocTransaction openWriteTransaction(long timeout, TimeUnit unit)
      throws TimeoutException {
    checkRunning();
    WriteDocTransaction decoratedTrans = decorated.openWriteTransaction(timeout, unit);
    boolean correct = false;
    try {
      WriteDocTransaction result = guard.createWriteTransaction(
          timeout,
          unit,
          () -> new ManagedWriteTransaction(decoratedTrans)
      );
      correct = true;

      return result;
    } finally {
      if (!correct) {
        decoratedTrans.close();
      }
    }
  }

  @Override
  public WriteDocTransaction openWriteTransaction() throws TimeoutException {
    checkRunning();
    return timeoutHandler.openWriteTransaction();
  }

  @Override
  public SchemaOperationExecutor openSchemaOperationExecutor(long timeout, TimeUnit unit)
      throws TimeoutException {
    checkRunning();
    SchemaOperationExecutor decoratedSchemaHandler =
        decorated.openSchemaOperationExecutor(timeout, unit);
    boolean correct = false;
    try {
      SchemaOperationExecutor result = guard.createSchemaHandler(
          timeout,
          unit,
          () -> new ManagedSchemaHandler(decoratedSchemaHandler)
      );
      correct = true;

      return result;
    } finally {
      if (!correct) {
        decoratedSchemaHandler.close();
      }
    }
  }

  @Override
  public SchemaOperationExecutor openSchemaOperationExecutor() throws TimeoutException {
    checkRunning();
    return timeoutHandler.openSchemaOperationExecutor();
  }

  private void checkRunning() {
    if (!isRunning()) {
      throw new IllegalStateException("This service is not running");
    }
  }

  private class ManagedReadTransaction extends DocTransactionDecorator<DocTransaction> {

    private boolean closed = false;

    public ManagedReadTransaction(DocTransaction trans) {
      super(trans);
    }

    @Override
    public synchronized void close() {
      if (!closed) {
        closed = true;
        getDecorated().close();
        guard.notifyTransactionClosed();
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

  private class ManagedWriteTransaction extends WriteDocTransactionDecorator {

    private boolean closed = false;

    public ManagedWriteTransaction(WriteDocTransaction trans) {
      super(trans);
    }

    @Override
    public synchronized void close() {
      if (!closed) {
        closed = true;
        getDecorated().close();
        guard.notifyTransactionClosed();
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

  private class ManagedSchemaHandler extends SchemaOperationExecutorDecorator {
    private boolean closed = false;

    public ManagedSchemaHandler(SchemaOperationExecutor decorated) {
      super(decorated);
    }

    @Override
    public synchronized void close() {
      if (!getDecorated().isClosed()) {
        closed = true;
        getDecorated().close();
        guard.notifySchemaHandlerClosed();
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