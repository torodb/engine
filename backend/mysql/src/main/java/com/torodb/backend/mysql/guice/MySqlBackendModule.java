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

package com.torodb.backend.mysql.guice;

import com.google.inject.PrivateModule;
import com.torodb.backend.BackendConfig;
import com.torodb.backend.DataTypeProvider;
import com.torodb.backend.DbBackendService;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.MetaDataReadInterface;
import com.torodb.backend.MetaDataWriteInterface;
import com.torodb.backend.ReadInterface;
import com.torodb.backend.StructureInterface;
import com.torodb.backend.WriteInterface;
import com.torodb.backend.ddl.DdlOpsModule;
import com.torodb.backend.ddl.DefaultReadStructure;
import com.torodb.backend.guice.BackendModule;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.mysql.MySqlDataTypeProvider;
import com.torodb.backend.mysql.MySqlDbBackend;
import com.torodb.backend.mysql.MySqlErrorHandler;
import com.torodb.backend.mysql.MySqlIdentifierConstraints;
import com.torodb.backend.mysql.MySqlMetaDataReadInterface;
import com.torodb.backend.mysql.MySqlMetaDataWriteInterface;
import com.torodb.backend.mysql.MySqlMetrics;
import com.torodb.backend.mysql.MySqlReadInterface;
import com.torodb.backend.mysql.MySqlStructureInterface;
import com.torodb.backend.mysql.MySqlWriteInterface;
import com.torodb.backend.mysql.driver.MySqlDriverProvider;
import com.torodb.backend.mysql.driver.OfficialMySqlDriver;
import com.torodb.backend.mysql.meta.MySqlReadStructure;
import com.torodb.backend.mysql.meta.MySqlSchemaUpdater;
import com.torodb.core.backend.BackendService;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.d2r.DefaultIdentifierFactory;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.d2r.UniqueIdentifierGenerator;
import com.torodb.core.guice.EssentialToDefaultModule;

import javax.inject.Singleton;

public class MySqlBackendModule extends PrivateModule {

  private final BackendConfig backendConfig;

  public MySqlBackendModule(BackendConfig backendConfig) {
    this.backendConfig = backendConfig;
  }

  @Override
  protected void configure() {
    expose(BackendService.class);
    expose(ReservedIdGenerator.class);

    install(new DdlOpsModule());
    install(new BackendModule());

    install(new EssentialToDefaultModule());

    bind(BackendConfig.class)
        .toInstance(backendConfig);
    bind(OfficialMySqlDriver.class)
        .in(Singleton.class);
    bind(MySqlDriverProvider.class)
        .to(OfficialMySqlDriver.class);

    bind(MySqlDbBackend.class)
        .in(Singleton.class);
    bind(DbBackendService.class)
        .to(MySqlDbBackend.class);
    expose(DbBackendService.class);

    bind(MySqlReadStructure.class);
    bind(DefaultReadStructure.class)
        .to(MySqlReadStructure.class);
    expose(DefaultReadStructure.class);

    bind(MySqlSchemaUpdater.class)
        .in(Singleton.class);
    bind(SchemaUpdater.class)
        .to(MySqlSchemaUpdater.class);
    expose(SchemaUpdater.class);

    bind(MySqlMetaDataReadInterface.class)
        .in(Singleton.class);
    bind(MetaDataReadInterface.class)
        .to(MySqlMetaDataReadInterface.class);
    expose(MetaDataReadInterface.class);

    bind(MySqlMetaDataWriteInterface.class)
        .in(Singleton.class);
    bind(MetaDataWriteInterface.class)
        .to(MySqlMetaDataWriteInterface.class);
    expose(MetaDataWriteInterface.class);

    bind(MySqlDataTypeProvider.class)
        .in(Singleton.class);
    bind(DataTypeProvider.class)
        .to(MySqlDataTypeProvider.class);
    expose(DataTypeProvider.class);

    bind(MySqlStructureInterface.class)
        .in(Singleton.class);
    bind(StructureInterface.class)
        .to(MySqlStructureInterface.class);
    expose(StructureInterface.class);

    bind(MySqlReadInterface.class)
        .in(Singleton.class);
    bind(ReadInterface.class)
        .to(MySqlReadInterface.class);
    expose(ReadInterface.class);

    bind(MySqlWriteInterface.class)
        .in(Singleton.class);
    bind(WriteInterface.class)
        .to(MySqlWriteInterface.class);
    expose(WriteInterface.class);

    bind(MySqlErrorHandler.class)
        .in(Singleton.class);
    bind(ErrorHandler.class)
        .to(MySqlErrorHandler.class);
    expose(ErrorHandler.class);

    bind(MySqlIdentifierConstraints.class)
        .in(Singleton.class);
    bind(IdentifierConstraints.class)
        .to(MySqlIdentifierConstraints.class);
    expose(IdentifierConstraints.class);

    bind(MySqlMetrics.class)
        .in(Singleton.class);

    bind(UniqueIdentifierGenerator.class)
        .in(Singleton.class);
    bind(DefaultIdentifierFactory.class)
        .in(Singleton.class);

    bind(IdentifierFactory.class)
        .to(DefaultIdentifierFactory.class);
    expose(IdentifierFactory.class);
  }

}
