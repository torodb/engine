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

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.ddl.ComposedDdlOperationExecutor;
import com.torodb.backend.service.BackendServiceImpl.DdlOperationExecutorFactory;
import com.torodb.backend.service.BackendServiceImpl.ReadDmlTransactionFactory;
import com.torodb.backend.service.BackendServiceImpl.WriteDmlTransactionFactory;
import com.torodb.core.TableRefFactory;
import com.torodb.core.backend.BackendService;
import com.torodb.core.d2r.IdentifierFactory;

import javax.inject.Singleton;

public class BackendServiceModule extends PrivateModule {

  @Override
  protected void configure() {
    expose(BackendService.class);

    bind(BackendServiceImpl.class)
        .in(Singleton.class);
    bind(BackendService.class)
        .to(BackendServiceImpl.class);
  }

  @Provides
  ReadDmlTransactionFactory createReadTransactionFactory(
      SqlInterface sqlInterface, KvMetainfoHandler metainfoHandler) {
    return () -> new ReadDmlTransactionImpl(sqlInterface, metainfoHandler);
  }

  @Provides
  WriteDmlTransactionFactory createWriteDmlTransactionFactory(
      SqlInterface sqlInterface, TableRefFactory tableRefFactory, IdentifierFactory idFactory,
      KvMetainfoHandler metainfoHandler) {
    return () -> new WriteDmlTransactionImpl(
        sqlInterface,
        tableRefFactory,
        idFactory,
        metainfoHandler
    );
  }

  @Provides
  DdlOperationExecutorFactory createDdlOperationExecutorFactory(SqlInterface sqlInterface) {
    return (ddlOps) -> new ComposedDdlOperationExecutor(sqlInterface, ddlOps);
  }

}