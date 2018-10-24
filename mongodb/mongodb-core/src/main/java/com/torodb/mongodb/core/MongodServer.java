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

import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.commands.CommandClassifier;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.CommandExecutor;
import com.torodb.mongowp.commands.Request;
import com.torodb.torod.DocTransaction;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.TorodServer;
import com.torodb.torod.WriteDocTransaction;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class MongodServer extends IdleTorodbService {

  private final Logger logger;
  private final TorodServer torodServer;
  private final ReadMongodTransactionFactory readTransFactory;
  private final WriteMongodTransactionFactory writeTransFactory;
  private final MongodSchemaExecutorFactory executorFactory;
  private final Supervisor supervisor;
  private final CommandExecutor<? super MongodServer> serverExecutor;

  @Inject
  public MongodServer(@TorodbIdleService ThreadFactory threadFactory,
      LoggerFactory loggerFactory,
      TorodServer torodServer,
      ReadMongodTransactionFactory readMongodTransactionFactory,
      WriteMongodTransactionFactory writeMongodTransactionFactory,
      MongodSchemaExecutorFactory executorFactory,
      CommandClassifier commandClassifier,
      Supervisor supervisor) {
    super(threadFactory);
    this.logger = loggerFactory.apply(this.getClass());
    this.torodServer = torodServer;
    this.readTransFactory = readMongodTransactionFactory;
    this.writeTransFactory = writeMongodTransactionFactory;
    this.executorFactory = executorFactory;
    this.supervisor = supervisor;
    this.serverExecutor = commandClassifier.getServerCommandsExecutor();
  }

  public TorodServer getTorodServer() {
    return torodServer;
  }

  public MongodTransaction openReadTransaction(long timeout, TimeUnit timeUnit)
      throws TimeoutException {
    return readTransFactory.createReadTransaction(
        torodServer.openReadTransaction(timeout, timeUnit)
    );
  }

  public MongodTransaction openReadTransaction() throws TimeoutException {
    return readTransFactory.createReadTransaction(
        torodServer.openReadTransaction()
    );
  }

  public WriteMongodTransaction openWriteTransaction(long timeout, TimeUnit timeUnit)
      throws InterruptedException, TimeoutException {
    return writeTransFactory.createWriteTransaction(
        torodServer.openWriteTransaction(timeout, timeUnit)
    );
  }

  public WriteMongodTransaction openWriteTransaction() throws TimeoutException {
    return writeTransFactory.createWriteTransaction(
        torodServer.openWriteTransaction()
    );
  }

  public MongodSchemaExecutor openSchemaExecutor(long timeout, TimeUnit timeUnit)
      throws InterruptedException, TimeoutException {
    return executorFactory.createMongodSchemaExecutor(
        torodServer.openSchemaOperationExecutor(timeout, timeUnit)
    );
  }

  public MongodSchemaExecutor openSchemaExecutor() throws TimeoutException {
    return executorFactory.createMongodSchemaExecutor(
        new MongoSchemaOperationExecutor(
            logger,
            torodServer.openSchemaOperationExecutor(),
            supervisor
        )
    );
  }

  @Override
  protected void startUp() throws Exception {
    logger.debug("Waiting for Torod server to be running");
    torodServer.awaitRunning();
    logger.debug("MongodServer ready to run");
  }

  @Override
  protected void shutDown() throws Exception {
  }

  public <A, R> Status<R> execute(Request req, Command<? super A, ? super R> command, A arg) {
    return serverExecutor.execute(req, command, arg, this);
  }

  static interface ReadMongodTransactionFactory {
    MongodTransaction createReadTransaction(DocTransaction docTrans);
  }

  static interface WriteMongodTransactionFactory {
    WriteMongodTransaction createWriteTransaction(WriteDocTransaction docTrans);
  }

  static interface MongodSchemaExecutorFactory {
    MongodSchemaExecutor createMongodSchemaExecutor(SchemaOperationExecutor torodSchemaOperationEx);
  }

}
