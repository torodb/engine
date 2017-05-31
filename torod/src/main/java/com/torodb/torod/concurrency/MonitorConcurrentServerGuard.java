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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Monitor;
import com.torodb.torod.DocTransaction;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.WriteDocTransaction;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 *
 */
class MonitorConcurrentServerGuard implements ConcurrentServerGuard {

  private final Monitor monitor;
  private final Monitor.Guard readyToTransaction;
  private final Monitor.Guard readyToSchema;

  private int openTransactions = 0;
  private int openSchemas = 0;

  public MonitorConcurrentServerGuard(boolean fair) {
    this.monitor = new Monitor(fair);
    readyToTransaction = new Monitor.Guard(monitor) {
      @Override
      public boolean isSatisfied() {
        return openSchemas == 0;
      }
    };
    readyToSchema = new Monitor.Guard(monitor) {
      @Override
      public boolean isSatisfied() {
        return openTransactions == 0 && openSchemas == 0;
      }
    };
  }


  @Override
  public void notifyTransactionClosed() {
    Preconditions.checkState(openTransactions > 0, "Trying to close transactions when there are "
        + "no open transactions");
    monitor.enter();
    try {
      openTransactions--;
    } finally {
      monitor.leave();
    }
  }

  @Override
  public void notifySchemaHandlerClosed() {
    Preconditions.checkState(openSchemas > 0, "Trying to close a schema handler when there are "
        + "no open schema handlers");
    monitor.enter();
    try {
      openSchemas--;
    } finally {
      monitor.leave();
    }
  }

  @Override
  public <T extends DocTransaction> T createTransaction(
      long timeout, TimeUnit unit, Supplier<T> supplier) throws TimeoutException {
    if (monitor.enterWhenUninterruptibly(readyToTransaction, timeout, unit)) {
      try {
        openTransactions++;
      } finally {
        monitor.leave();
      }
      return supplier.get();
    } else {
      throw new TimeoutException("Impossible to open a transaction in " + timeout);
    }
  }

  @Override
  public <T extends WriteDocTransaction> WriteDocTransaction createWriteTransaction(
      long timeout, TimeUnit unit, Supplier<T> supplier) throws TimeoutException {
    return createTransaction(timeout, unit, supplier);
  }

  @Override
  public <S extends SchemaOperationExecutor> S createSchemaHandler(
      long timeout, TimeUnit unit, Supplier<S> supplier) throws TimeoutException {
    if (monitor.enterWhenUninterruptibly(readyToSchema, timeout, unit)) {
      try {
        openSchemas++;
      } finally {
        monitor.leave();
      }
      return supplier.get();
    } else {
      throw new TimeoutException("Impossible to open a schema handler in " + timeout);
    }
  }

}