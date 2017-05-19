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

import static org.mockito.Mockito.when;

import org.jooq.lambda.Unchecked;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

@RunWith(JUnitPlatform.class)
public abstract class TorodServerContract {

  private TorodServer server;
  private ProtectedServer decorated;
  private static ExecutorService executorService;

  @BeforeAll
  static void beforeAll() {
    executorService = Executors.newCachedThreadPool();
  }

  @AfterAll
  static void afterAll() {
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  @BeforeEach
  void setUp() throws InterruptedException, TimeoutException {
    decorated = Mockito.mock(ProtectedServer.class);

    when(decorated.openReadTransaction(Mockito.anyLong(), Mockito.any()))
        .then(this::createReadTransaction);
    when(decorated.openWriteTransaction(Mockito.anyLong(), Mockito.any()))
        .then(this::createWriteTransaction);
    when(decorated.openSchemaOperationExecutor(Mockito.anyLong(), Mockito.any()))
        .then(this::createSchemaHandler);

    server = createServer(decorated);
    server.startAsync();
    server.awaitRunning();
  }

  @AfterEach
  void tearDown() {
    server.stopAsync();
    server.awaitTerminated();
  }

  private <V> Future<V> submit(Callable<V> callable) {
    return executorService.submit(callable);
  }

  protected abstract TorodServer createServer(ProtectedServer decorated);

  @Nested
  class InitialState {

    @Test
    void openReadTransaction() {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        try (AutoCloseable ac = server.openReadTransaction(openReadTransactionMillisTimeout(), TimeUnit.MILLISECONDS)) {
        }
      });
    }

    @Test
    void openWriteTransaction() {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        try (AutoCloseable ac = server.openWriteTransaction(openWriteTransactionTimeout(), TimeUnit.MILLISECONDS)) {
        }
      });
    }

    @Test
    void openSchemaHandler() {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        try (AutoCloseable ac = server.openSchemaOperationExecutor(openSchemaHandlerMillisTimeout(), TimeUnit.MILLISECONDS)) {
        }
      });
    }

  }

  abstract class OnTransaction {

    private Closer closer;

    abstract DocTransaction createTransaction() throws Exception;

    @BeforeEach
    void setUp() throws Exception {
      closer = new Closer(this::createTransaction);
    }

    @AfterEach
    void tearDown() {
      if (closer != null) {
        closer.close();
      }
    }

    @Test
    void openReadTransaction() {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        try (AutoCloseable ac = server.openReadTransaction(openReadTransactionMillisTimeout(), TimeUnit.MILLISECONDS)){

        }
      });
    }

    @Test
    void openWriteTransaction() {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        try (AutoCloseable ac = server.openWriteTransaction(openWriteTransactionTimeout(), TimeUnit.MILLISECONDS)) {
          
        }
      });
    }

    @Test
    void timeoutOnOpenSchemaHandler() {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        Assertions.assertThrows(
            TimeoutException.class,
            TorodServerContract.this::openSchemaHandler
        );
      });
    }

    @Test
    void unlockOnOpenSchemaHandler() throws Exception {
      assertUnlock(
          TorodServerContract.this::openSchemaHandler,
          closer::close
      );
    }

  }

  @Nested
  class OnReadTransaction extends OnTransaction {

    @Override
    DocTransaction createTransaction() throws Exception {
      return server.openReadTransaction(openReadTransactionMillisTimeout(), TimeUnit.MILLISECONDS);
    }

  }

  @Nested
  class OnWriteTransaction extends OnTransaction {

    @Override
    DocTransaction createTransaction() throws Exception {
      return server.openWriteTransaction(openWriteTransactionTimeout(), TimeUnit.MILLISECONDS);
    }

  }

  @Nested
  class OnSchemaHandler {

    private Closer closer;

    @BeforeEach
    void setUp() throws Exception {
      closer = new Closer(this::createSchemaHandler);
    }

    private SchemaOperationExecutor createSchemaHandler() throws InterruptedException, TimeoutException {
      return server.openSchemaOperationExecutor(openSchemaHandlerMillisTimeout(), TimeUnit.MILLISECONDS);
    }

    @AfterEach
    void tearDown() {
      if (closer != null) {
        closer.close();
      }
    }

    @Test
    void timeoutOpenReadTransaction() {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        Assertions.assertThrows(
            TimeoutException.class,
            TorodServerContract.this::openReadTransaction
        );
      });
    }

    @Test
    void unlockOnOpenReadTransaction() throws Exception {
      assertUnlock(
          TorodServerContract.this::openReadTransaction,
          closer::close
      );
    }

    @Test
    void timeoutOpenWriteTransaction() {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        Assertions.assertThrows(
            TimeoutException.class,
            TorodServerContract.this::openWriteTransaction
        );
      });
    }

    @Test
    void unlockOnOpenWriteTransaction() throws Exception {
      assertUnlock(
          TorodServerContract.this::openWriteTransaction,
          closer::close
      );
    }

    @Test
    void timeoutOnOpenSchemaHandler() {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        Assertions.assertThrows(
            TimeoutException.class,
            TorodServerContract.this::openSchemaHandler
        );
      });
    }

    @Test
    void unlockOnOpenSchemaHandler() throws Exception {
      assertUnlock(
          TorodServerContract.this::openSchemaHandler,
          closer::close
      );
    }
  }

  protected static interface UnblockAction {

    void unblock();
  }

  /**
   * Executes the blocking operation on a different thread, then executes unblock and waits until
   * the blocking operation finishes.
   *
   * This method does not check that the blocking operation is blocked until the unblocked is
   * executed. Another test should check that.
   *
   * @param timeout          The maximum time this method should be working before throwing a
   *                         timeout
   * @param blockedOperation The blocking operation. The returned closable will be closed
   * @param unblockAction    The action that unblocks the blocking operation
   */
  protected void assertUnlock(
      Duration timeout,
      Callable<? extends AutoCloseable> blockedOperation,
      UnblockAction unblockAction) {

    Assertions.assertTimeoutPreemptively(timeout, () -> {
      //This future shouldn't be able to finish
      Future<? extends AutoCloseable> blockedFuture = submit(blockedOperation);

      //Until the transaction is closed
      unblockAction.unblock();

      AutoCloseable autoCloseable = blockedFuture.get();
      if (autoCloseable != null) {
        autoCloseable.close();
      }
    });
  }

  protected void assertUnlock(
      Callable<? extends AutoCloseable> blockedOperation,
      UnblockAction unblockAction) {
    assertUnlock(getTestTimeout(), blockedOperation, unblockAction);
  }

  protected long openSchemaHandlerMillisTimeout() {
    return 2000;
  }

  protected long openTransactionMillisTimeout() {
    return 2000;
  }

  protected long openReadTransactionMillisTimeout() {
    return openTransactionMillisTimeout();
  }

  protected long openWriteTransactionTimeout() {
    return openTransactionMillisTimeout();
  }

  protected DocTransaction openReadTransaction() throws InterruptedException, TimeoutException {
    return server.openReadTransaction(openReadTransactionMillisTimeout(), TimeUnit.MILLISECONDS);
  }

  protected WriteDocTransaction openWriteTransaction() throws InterruptedException, TimeoutException {
    return server.openWriteTransaction(openWriteTransactionTimeout(), TimeUnit.MILLISECONDS);
  }

  protected SchemaOperationExecutor openSchemaHandler() throws InterruptedException, TimeoutException {
    return server.openSchemaOperationExecutor(openSchemaHandlerMillisTimeout(), TimeUnit.MILLISECONDS);
  }

  private DocTransaction createReadTransaction(InvocationOnMock invocation) {
    DocTransaction mock = Mockito.mock(DocTransaction.class, Answers.RETURNS_SMART_NULLS);
    MyCloseable closeable = new MyCloseable();
    Mockito.doAnswer((inv) -> {
      closeable.close();
      return null;
    })
        .when(mock)
        .close();
    Mockito.doAnswer((inv) -> closeable.isClosed())
        .when(mock)
        .isClosed();

    return mock;
  }

  private WriteDocTransaction createWriteTransaction(InvocationOnMock invocation) {
    WriteDocTransaction mock = Mockito.mock(WriteDocTransaction.class, Answers.RETURNS_SMART_NULLS);
    MyCloseable closeable = new MyCloseable();
    Mockito.doAnswer((inv) -> {
      closeable.close();
      return null;
    })
        .when(mock)
        .close();
    Mockito.doAnswer((inv) -> closeable.isClosed())
        .when(mock)
        .isClosed();

    return mock;
  }

  private SchemaOperationExecutor createSchemaHandler(InvocationOnMock invocation) {
    SchemaOperationExecutor mock = Mockito.mock(SchemaOperationExecutor.class, Answers.RETURNS_SMART_NULLS);
    MyCloseable closeable = new MyCloseable();
    Mockito.doAnswer((inv) -> {
      closeable.close();
      return null;
    })
        .when(mock)
        .close();
    Mockito.doAnswer((inv) -> closeable.isClosed())
        .when(mock)
        .isClosed();

    return mock;
  }

  /**
   * A general <em>high enough</em> duration that can be used on simple test to avoid deadlocks on
   * erroneous code.
   *
   * <p>
   * This duration is higher than default timeouts of {@link #openReadTransaction() },
   * {@link #openWriteTransaction() } and {@link #openSchemaHandler() }, so on typical situations, a
   * test that uses this method as timeout can execute at least one of these methods without
   * producing a false negative on the test because a timeout. Anormal situations (like stop the JVM
   * meanwhile the test is executed) can still produce false negatives.
   */
  private Duration getTestTimeout() {
    return Stream.of(openReadTransactionMillisTimeout(),
        openSchemaHandlerMillisTimeout(),
        openWriteTransactionTimeout(),
        Duration.ofMinutes(10).toMillis()
    )
        .max(Long::compare)
        .map(t -> t + 500)
        .map(Duration::ofMillis)
        .orElse(Duration.ofSeconds(1));
  }

  private class Closer {

    private final AutoCloseable resource;

    public Closer(Callable<AutoCloseable> resourceSupplier)
        throws InterruptedException, ExecutionException {
      resource = executorService.submit(resourceSupplier).get();
    }

    public void close() {
      CompletableFuture.runAsync(
          Unchecked.runnable(resource::close),
          executorService
      ).join();
    }
  }

  private static class MyCloseable implements AutoCloseable {
    private boolean closed = false;

    @Override
    public void close() throws Exception {
      closed = true;
    }

    public boolean isClosed() {
      return closed;
    }
  }

}