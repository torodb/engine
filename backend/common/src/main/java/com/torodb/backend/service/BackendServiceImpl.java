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

package com.torodb.backend.service;

import com.torodb.backend.BackendLoggerFactory;
import com.torodb.backend.DbBackendService;
import com.torodb.backend.ddl.DdlOps;
import com.torodb.core.backend.BackendService;
import com.torodb.core.backend.DdlOperationExecutor;
import com.torodb.core.backend.DmlTransaction;
import com.torodb.core.backend.WriteDmlTransaction;
import com.torodb.core.services.IdleTorodbService;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ThreadFactory;

import javax.inject.Inject;

public class BackendServiceImpl extends IdleTorodbService implements BackendService {

  private static final Logger LOGGER = BackendLoggerFactory.get(BackendServiceImpl.class);

  private final DbBackendService dbBackendService;
  private final DdlOps ddlOps;
  private final ReadDmlTransactionFactory readFactory;
  private final WriteDmlTransactionFactory writeFactory;
  private final DdlOperationExecutorFactory ddlOpExFactory;

  @Inject
  public BackendServiceImpl(DbBackendService dbBackendService, DdlOps ddlOps,
      ReadDmlTransactionFactory readFactory, WriteDmlTransactionFactory writeFactory,
      DdlOperationExecutorFactory ddlOpExFactory, ThreadFactory threadFactory) {
    super(threadFactory);
    this.dbBackendService = dbBackendService;
    this.ddlOps = ddlOps;
    this.readFactory = readFactory;
    this.writeFactory = writeFactory;
    this.ddlOpExFactory = ddlOpExFactory;
  }

  @Override
  public DmlTransaction openReadTransaction() {
    return readFactory.newReadTransaction();
  }

  @Override
  public WriteDmlTransaction openWriteTransaction() {
    return writeFactory.newWriteTransaction();
  }

  @Override
  public DdlOperationExecutor openDdlOperationExecutor() {
    return ddlOpExFactory.newOperationExecutor(ddlOps);
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.debug("Starting backend...");

    LOGGER.trace("Waiting for {} to be running...", dbBackendService);
    dbBackendService.awaitRunning();

    LOGGER.debug("Backend started");
  }

  @Override
  protected void shutDown() throws Exception {
    ddlOps.close();
  }

  static interface ReadDmlTransactionFactory {
    ReadDmlTransactionImpl newReadTransaction();
  }

  static interface WriteDmlTransactionFactory {
    WriteDmlTransactionImpl newWriteTransaction();
  }

  static interface DdlOperationExecutorFactory {
    DdlOperationExecutor newOperationExecutor(DdlOps ddlOps);
  }
}