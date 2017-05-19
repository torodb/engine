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

package com.torodb.mongodb.repl.sharding.isolation.db;

import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.torod.DocTransaction;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.TorodServer;
import com.torodb.torod.WriteDocTransaction;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class DbIsolatorServer extends IdleTorodbService implements TorodServer {

  private final Logger logger;
  private final Converter converter;
  private final TorodServer decorated;

  public DbIsolatorServer(String shardId, TorodServer decorated, ThreadFactory threadFactory,
      LoggerFactory lf) {
    super(threadFactory);
    this.logger = lf.apply(this.getClass());
    assert decorated.isRunning() : "The decorated torod server must be running";
    this.decorated = decorated;
    this.converter = new Converter(shardId);
  }

  private DocTransaction decorate(DocTransaction trans) {
    return new DbIsolatorTrans<DocTransaction>(converter, trans);
  }

  private WriteDocTransaction decorate(WriteDocTransaction trans) {
    return new DbIsolatorWriteTrans(converter, trans);
  }

  private SchemaOperationExecutor decorate(SchemaOperationExecutor schemaEx) {
    return new DbIsolatorSchemaOperationExecutor(converter, schemaEx);
  }

  @Override
  public DocTransaction openReadTransaction(long timeout, TimeUnit unit) throws TimeoutException {
    checkRunning();
    DocTransaction toDecorate = decorated.openReadTransaction(timeout, unit);
    return decorate(toDecorate);
  }

  @Override
  public DocTransaction openReadTransaction() throws TimeoutException {
    checkRunning();
    DocTransaction toDecorate = decorated.openReadTransaction();
    return decorate(toDecorate);
  }

  @Override
  public WriteDocTransaction openWriteTransaction(long timeout, TimeUnit unit) throws
      TimeoutException {
    checkRunning();
    WriteDocTransaction toDecorate = decorated.openWriteTransaction(timeout, unit);
    return decorate(toDecorate);
  }

  @Override
  public WriteDocTransaction openWriteTransaction() throws TimeoutException {
    checkRunning();
    WriteDocTransaction toDecorate = decorated.openWriteTransaction();
    return decorate(toDecorate);
  }

  @Override
  public SchemaOperationExecutor openSchemaOperationExecutor(long timeout, TimeUnit unit) throws
      TimeoutException {
    checkRunning();
    SchemaOperationExecutor toDecorate = decorated.openSchemaOperationExecutor(timeout, unit);
    return decorate(toDecorate);
  }

  @Override
  public SchemaOperationExecutor openSchemaOperationExecutor() throws TimeoutException {
    checkRunning();
    SchemaOperationExecutor toDecorate = decorated.openSchemaOperationExecutor();
    return decorate(toDecorate);
  }

  @Override
  protected void startUp() throws Exception {
  }

  @Override
  protected void shutDown() throws Exception {
  }

  private void checkRunning() {
    if (!isRunning()) {
      throw new IllegalStateException("The isolator server is not running");
    }
  }

}
