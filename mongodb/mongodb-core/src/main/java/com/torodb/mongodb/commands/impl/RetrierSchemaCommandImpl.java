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

package com.torodb.mongodb.commands.impl;

import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.retrier.DefaultRetrier;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.mongowp.ErrorCode;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.Request;
import com.torodb.torod.SchemaOperationExecutor;
import org.apache.logging.log4j.Logger;

import java.util.EnumSet;
import java.util.concurrent.Callable;

/**
 *
 */
public abstract class RetrierSchemaCommandImpl<A, R> implements SchemaCommandImpl<A, R> {

  private final Logger logger;
  private final Retrier retrier;

  public RetrierSchemaCommandImpl(LoggerFactory loggerFactory, Retrier retrier) {
    this.logger = loggerFactory.apply(this.getClass());
    this.retrier = retrier;
  }

  public RetrierSchemaCommandImpl(LoggerFactory loggerFactory) {
    this.logger = loggerFactory.apply(this.getClass());
    this.retrier = DefaultRetrier.getInstance();
  }

  protected abstract Status<R> tryApply(Request req, Command<? super A, ? super R> command,
      A arg, SchemaOperationExecutor context) throws RetrierGiveUpException, RollbackException;

  protected EnumSet<Retrier.Hint> getHints() {
    return EnumSet.of(Retrier.Hint.TIME_SENSIBLE);
  }

  protected Logger getLogger() {
    return logger;
  }

  protected Retrier getRetrier() {
    return retrier;
  }

  protected String getErrorMessage(Request req, Command<? super A, ? super R> command,
      A arg) {
    return "Error while trying to execute " + command.getCommandName() + " on db "
        + req.getDatabase();
  }

  @Override
  public Status<R> apply(Request req, Command<? super A, ? super R> command,
      A arg, SchemaOperationExecutor context) {
    try {
      return retrier.retry(new Callable<Status<R>>() {
        @Override
        public Status<R> call() throws Exception {
          return tryApply(req, command, arg, context);
        }
      }, getHints());
    } catch (RetrierGiveUpException ex) {
      logger.debug(getErrorMessage(req, command, arg), ex);
      return Status.from(ErrorCode.COMMAND_FAILED);
    }
  }

}