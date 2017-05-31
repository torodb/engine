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

package com.torodb.mongodb.core;

import com.torodb.core.transaction.RollbackException;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.Request;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;

abstract class MongodTransactionImpl implements MongodTransaction {

  private final Logger logger;

  private boolean closed = false;

  MongodTransactionImpl(Function<Class<?>, Logger> loggerFactory) {
    this.logger = loggerFactory.apply(this.getClass());
  }

  protected abstract <A, R> Status<R> executeProtected(Request req,
      Command<? super A, ? super R> command, A arg);

  @Override
  public <A, R> Status<R> execute(Request req,
      Command<? super A, ? super R> command, A arg) throws RollbackException {
    Status<R> status = executeProtected(req, command, arg);
    return status;
  }

  @Override
  public void rollback() {
    getDocTransaction().rollback();
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      getDocTransaction().close();
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  protected void finalize() throws Throwable {
    if (!closed) {
      logger.warn(this.getClass() + " finalized without being closed");
      close();
    }
  }

}
