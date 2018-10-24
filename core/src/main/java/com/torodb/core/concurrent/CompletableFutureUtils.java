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

package com.torodb.core.concurrent;

import com.google.common.base.Throwables;
import com.torodb.common.util.CompletionExceptions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import javax.inject.Inject;

/**
 *
 */
public class CompletableFutureUtils {

  private final ScheduledExecutorService delayer;

  @Inject
  public CompletableFutureUtils(ConcurrentToolsFactory concurrentToolsFactory) {
    delayer = concurrentToolsFactory.createScheduledExecutorServiceWithMaxThreads(
        "future-timeout",
        Runtime.getRuntime().availableProcessors()
    );
  }

  public <R> CompletableFuture<R> orTimeout(long delay, TimeUnit unit) {
    CompletableFuture<R> result = new CompletableFuture<>();
    delayer.schedule(
        () -> result.completeExceptionally(new TimeoutException()),
        delay,
        unit
    );
    return result;
  }

  public <R> R executeOrTimeout(CompletableFuture<R> future, long delay, TimeUnit unit)
      throws TimeoutException, CompletionException {

    try {
      return future.applyToEither(orTimeout(delay, unit), Function.identity())
          .join();
    } catch (CompletionException ex) {
      Throwables.throwIfInstanceOf(
          CompletionExceptions.getFirstNonCompletionException(ex),
          TimeoutException.class
      );
      throw ex;
    }

  }

}