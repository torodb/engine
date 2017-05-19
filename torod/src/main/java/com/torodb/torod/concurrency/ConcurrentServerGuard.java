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
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.WriteDocTransaction;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * A concurrency utility used to control the document transactions and schema handlers construction.
 *
 * {@link ConcurrentTorodServer} delegates concurrency decisions to this interface to improve the
 * <a href="https://en.wikipedia.org/wiki/Single_responsibility_principle"> single responsibility
 * principle</a>. ConcurrentTorodServer is responsible for how to open and close transactions and
 * handlers and this object is responsible for the concurrency exclusion between transactions and
 * handlers.
 */
interface ConcurrentServerGuard {

  /**
   * This method must be called each time a {@link DocTransaction} returned by this class is
   * closed.
   * @throws IllegalStateException If the guard thinks there are no open transactions.
   */
  void notifyTransactionClosed() throws IllegalStateException;

  /**
   * This method must be called each time a {@link SchemaOperationExecutor} returned by this class
   * is closed.
   *
   * @throws IllegalStateException If the guard thinks there are no open schema handlers.
   */
  void notifySchemaHandlerClosed() throws IllegalStateException;

  /**
   * Creates a transaction by using the supplier.
   *
   * The supplier will not be called (at least) until there are no open
   * {@link SchemaOperationExecutor schema handlers}.
   *
   * @param timeout  the maximum time this thread should be blocked.
   * @param unit     the unit on which the timeout is specified
   * @param supplier the function used to create the transaction
   * @return the created transaction. It is important to use this object and not the one created by
   *         the supplier
   * @throws TimeoutException     if the guard decides to not create the transaction on the given
   *                              time
   * @throws InterruptedException if the thread is interrupted
   */
  <T extends DocTransaction> T createTransaction(
      long timeout, TimeUnit unit, Supplier<T> supplier) throws TimeoutException;

  /**
   * Like {@link #createTransaction(java.time.Duration, java.util.function.Supplier) }, but creates
   * a {@link WriteDocTransaction}.
   */
  <T extends WriteDocTransaction> WriteDocTransaction createWriteTransaction(
      long timeout, TimeUnit unit, Supplier<T> supplier) throws TimeoutException;

  /**
   * Creates a schema handler by using the supplier.
   *
   * The supplier will not be called (at least) until there are no open
   * {@link DocTransaction document transactions}.
   *
   * @param timeout  the maximum time this thread should be blocked.
   * @param unit     the unit on which the timeout is specified
   * @param supplier the function used to create the schema handler
   * @return the created schema handler. It is important to use this object and not the one created
   *         by the supplier
   * @throws TimeoutException     if the guard decides to not create the schema handler on the given
   *                              time
   * @throws InterruptedException if the thread is interrupted
   */
  <S extends SchemaOperationExecutor> SchemaOperationExecutor createSchemaHandler(
      long timeout, TimeUnit unit, Supplier<S> supplier) throws TimeoutException;

}