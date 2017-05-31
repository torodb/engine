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

import com.torodb.torod.DocTransaction;
import com.torodb.torod.WriteDocTransaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.torodb.torod.SchemaOperationExecutor;

import java.util.concurrent.TimeUnit;

@RunWith(JUnitPlatform.class)
public abstract class ConcurrentServerGuardContract {

  private ConcurrentServerGuard guard;
  private Helper helper;

  private static WriteDocTransaction transaction = Mockito.mock(WriteDocTransaction.class);
  private static SchemaOperationExecutor schemaHandler = Mockito.mock(SchemaOperationExecutor.class);

  private static final Supplier<WriteDocTransaction> transSupplier = () -> transaction;
  private static final Supplier<SchemaOperationExecutor> shSupplier = () -> schemaHandler;

  protected abstract ConcurrentServerGuard createGuard();

  @BeforeEach
  void setUp() {
    guard = createGuard();
    helper = new Helper();
  }

  @Nested
  class InitialState {

    @Test
    void testNotifyTransactionClosed() {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        Assertions.assertThrows(IllegalStateException.class, () -> {
          guard.notifyTransactionClosed();
        });
      });
    }

    @Test
    void testNotifySchemaHandlerClosed() {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        Assertions.assertThrows(IllegalStateException.class, () -> {
          guard.notifySchemaHandlerClosed();
        });
      });
    }

    @Test
    void testCreateTransaction() throws Exception {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), helper::createTransactionAndClose);
    }

    @Test
    void testCreateSchemaHandler() throws Exception {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), helper::createSchemaHandlerAndClose);
    }

  }

  @Nested
  class OnTransaction {

    private DocTransaction transaction;

    @BeforeEach
    void setUp() throws TimeoutException, InterruptedException {
      transaction = helper.createTransaction();
    }

    @AfterEach
    void tearDown() {
      if (transaction != null) {
        transaction.close();
      }
    }

    @Test
    void expectedNotifyTransactionClosed() {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), guard::notifyTransactionClosed);
    }

    @Test
    void unexpectedNotifySchemaHandlerClosed() {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        Assertions.assertThrows(IllegalStateException.class, guard::notifySchemaHandlerClosed);
      });
    }

    @Test
    void expectedCreateTransaction() throws Exception {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), helper::createTransactionAndClose);
    }

    @Test
    void timeoutCreateSchemaHandler() throws Exception {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        Assertions.assertThrows(TimeoutException.class, helper::createSchemaHandlerAndClose);
      });
    }

    @Test
    void unlockCreateSchemaHandler() throws Exception {

      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        //This future shouldn't be able to finish
        Future<?> createShFuture = submit(helper::createTransactionAndClose);

        //Until the active transaction is closed
        guard.notifyTransactionClosed();

        createShFuture.get();
      });

    }

  }

  @Nested
  class OnSchema {

    private SchemaOperationExecutor schemaHandler;

    @BeforeEach
    void setUp() throws TimeoutException, InterruptedException {
      schemaHandler = helper.createSchemaHandler();
    }

    @AfterEach
    void tearDown() {
      if (schemaHandler != null) {
        schemaHandler.close();
      }
    }

    @Test
    void unexpectedNotifyTransactionClosed() {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        Assertions.assertThrows(IllegalStateException.class, guard::notifyTransactionClosed);
      });
    }

    @Test
    void expectedNotifySchemaHandlerClosed() {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), guard::notifySchemaHandlerClosed);
    }

    @Test
    void timeoutCreateTransaction() throws Exception {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        Assertions.assertThrows(TimeoutException.class, helper::createTransactionAndClose);
      });
    }

    @Test
    void timeoutCreateSchemaHandler() throws Exception {
      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        Assertions.assertThrows(TimeoutException.class, helper::createSchemaHandlerAndClose);
      });
    }

    @Test
    void unlockCreateTransaction() throws Exception {

      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        //This future shouldn't be able to finish
        Future<?> future = submit(helper::createTransactionAndClose);

        //Until the active schema is closed
        guard.notifySchemaHandlerClosed();

        future.get();
      });

    }

    @Test
    void unlockCreateSchemaHandler() throws Exception {

      Assertions.assertTimeoutPreemptively(getTestTimeout(), () -> {
        //This future shouldn't be able to finish
        Future<?> future = submit(helper::createSchemaHandlerAndClose);

        //Until the active schema is closed
        guard.notifySchemaHandlerClosed();

        future.get();
      });

    }

  }

  class Helper {

    protected DocTransaction createTransaction() throws InterruptedException, TimeoutException {
      return guard.createTransaction(
          createTransactionMillisTimeout(),
          TimeUnit.MILLISECONDS,
          transSupplier);
    }

    protected SchemaOperationExecutor createSchemaHandler()
        throws InterruptedException, TimeoutException {
      return guard.createSchemaHandler(
          createSchemaHandlerMillisTimeout(),
          TimeUnit.MILLISECONDS,
          shSupplier);
    }

    protected Boolean createTransactionAndClose() throws InterruptedException, TimeoutException {
      WriteDocTransaction trans = guard.createTransaction(
          createTransactionMillisTimeout(),
          TimeUnit.MILLISECONDS, transSupplier);
      trans.close();
      return true;
    }

    protected Boolean createSchemaHandlerAndClose() throws InterruptedException, TimeoutException {
      SchemaOperationExecutor sh = guard.createSchemaHandler(
          createSchemaHandlerMillisTimeout(),
          TimeUnit.MILLISECONDS, shSupplier);
      sh.close();
      return true;
    }
  }

  protected long createSchemaHandlerMillisTimeout() {
    return 2000;
  }

  protected long createTransactionMillisTimeout() {
    return 2000;
  }

  private <V> Future<V> submit(Callable<V> callable) {
    return ForkJoinPool.commonPool().submit(callable);
  }

  /**
   * A general <em>high enough</em> duration that can be used on simple test to avoid deadlocks on
   * erroneous code.
   *
   * <p>
   * This duration is higher than default timeouts of {@link #createSchemaHandlerMillisTimeout() }
   * and {@link #createTransactionMillisTimeout() }, so on typical situations, a test that uses this
   * method as timeout can execute at least one of these methods without producing a false negative
   * on the test because a timeout. Anormal situations (like stop the JVM meanwhile the test is
   * executed) can still produce false negatives.
   */
  private Duration getTestTimeout() {
    return Stream.of(createSchemaHandlerMillisTimeout(),
        createTransactionMillisTimeout()
    )
        .max(Long::compare)
        .map(t -> t + 500)
        .map(Duration::ofMillis)
        .orElse(Duration.ofSeconds(1));
  }
}