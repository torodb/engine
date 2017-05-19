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

package com.torodb.torod.impl.sql;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.torodb.core.TableRefFactory;
import com.torodb.core.backend.BackendExtInt;
import com.torodb.core.backend.BackendService;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.d2r.impl.D2RModule;
import com.torodb.core.guice.EssentialToDefaultModule;
import com.torodb.core.supervision.Supervisor;
import com.torodb.torod.ProtectedServer;
import com.torodb.torod.impl.sql.schema.SqlSchemaModule;


public class SqlTorodModule extends PrivateModule {

  private final SqlTorodConfig config;

  public SqlTorodModule(SqlTorodConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    expose(ProtectedServer.class);

    bindConfig();

    install(new EssentialToDefaultModule());
    install(new D2RModule());
    install(new SqlSchemaModule());

    bind(Supervisor.class)
        .toInstance(config.getSupervisor());

    bind(ProtectedServer.class)
        .to(SqlTorodServer.class)
        .in(Singleton.class);

    bind(InsertD2RTranslator.class);
  }

  private void bindConfig() {
    BackendExtInt backendExtInt = config.getBackendBundle().getExternalInterface();

    bind(BackendService.class)
        .toInstance(backendExtInt.getBackendService());
    bind(IdentifierFactory.class)
        .toInstance(backendExtInt.getIdentifierFactory());
    bind(ReservedIdGenerator.class)
        .toInstance(backendExtInt.getReservedIdGenerator());
  }

  @Provides
  SqlTorodServer.ReadDocTransactionFactory createReadDocTransactionFactory(
      R2DTranslator r2d, TableRefFactory tableRefFactory) {
    return (dmlTrans, snapshot) -> new SqlTransaction<>(dmlTrans, snapshot, r2d, tableRefFactory);
  }

  @Provides
  SqlTorodServer.WriteDocTransactionFactory createWriteDocTransactionFactory(
      R2DTranslator r2d, TableRefFactory tableRefFactory, InsertD2RTranslator insertAnalyzer) {
    return (dmlTrans, snapshot, prepareSchemaCallback) -> new SqlWriteTransaction(
        dmlTrans,
        snapshot,
        r2d,
        tableRefFactory,
        insertAnalyzer,
        prepareSchemaCallback
    );
  }
}