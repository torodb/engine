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

package com.torodb.backend.mysql;

import com.torodb.backend.*;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.mysql.MySqlDataTypeProvider;
import com.torodb.backend.mysql.MySqlDbBackend;
import com.torodb.backend.mysql.MySqlErrorHandler;
import com.torodb.backend.mysql.MySqlIdentifierConstraints;
import com.torodb.backend.mysql.MySqlMetaDataReadInterface;
import com.torodb.backend.mysql.MySqlMetaDataWriteInterface;
import com.torodb.backend.mysql.MySqlStructureInterface;
import com.torodb.backend.mysql.driver.OfficialMySqlDriver;
import com.torodb.backend.mysql.driver.MySqlDriverProvider;
import com.torodb.backend.mysql.meta.MySqlSchemaUpdater;
import com.torodb.backend.tests.common.DatabaseTestContext;
import com.torodb.backend.tests.common.IntegrationTestBundleConfig;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.d2r.UniqueIdentifierGenerator;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class MySqlDatabaseTestContextFactory {

  public DatabaseTestContext createInstance() {
    MySqlDataTypeProvider dataTypeProvider = new MySqlDataTypeProvider();
    MySqlErrorHandler errorHandler = new MySqlErrorHandler();
    SqlHelper sqlHelper = new SqlHelper(dataTypeProvider, errorHandler);

    BundleConfig bundleConfig = new IntegrationTestBundleConfig();
    BackendConfig backendConfig = new BackendConfigImplBuilder(bundleConfig)
        .setUsername("test")
        .setPassword("test")
        .setDbHost("localhost")
        .setDbName("test")
        .setDbPort(13306)
        .setIncludeForeignKeys(false)
        .build();

    DslContextFactory dslContextFactory = new DslContextFactoryImpl(dataTypeProvider);
    SqlInterface sqlInterface =
        buildSqlInterface(dataTypeProvider, sqlHelper, errorHandler, backendConfig, dslContextFactory);


    SchemaUpdater schemaUpdater = new MySqlSchemaUpdater(sqlInterface, sqlHelper);

    return new MySqlDatabaseTestContext(sqlInterface, dslContextFactory, schemaUpdater);
  }

  private SqlInterface buildSqlInterface(MySqlDataTypeProvider dataTypeProvider, SqlHelper sqlHelper,
                                         MySqlErrorHandler errorHandler,
                                         BackendConfig backendConfig,
                                         DslContextFactory dslContextFactory) {
    MySqlDriverProvider driver = new OfficialMySqlDriver();
    ThreadFactory threadFactory = Executors.defaultThreadFactory();

    IdentifierConstraints identifierConstraints = new MySqlIdentifierConstraints(backendConfig);
    UniqueIdentifierGenerator uniqueIdentifierGenerator = new UniqueIdentifierGenerator(identifierConstraints);
    MySqlDbBackend dbBackend = new MySqlDbBackend(threadFactory, backendConfig, driver, errorHandler);

    MySqlMetaDataReadInterface metaDataReadInterface = new MySqlMetaDataReadInterface(sqlHelper);
    
    MySqlStructureInterface structureInterface =
        new MySqlStructureInterface(dbBackend, metaDataReadInterface, sqlHelper, 
            identifierConstraints, dataTypeProvider, uniqueIdentifierGenerator);

    MySqlMetaDataWriteInterface metadataWriteInterface =
        new MySqlMetaDataWriteInterface(metaDataReadInterface, sqlHelper);

    dbBackend.startAsync();
    dbBackend.awaitRunning();

    return new SqlInterfaceDelegate(metaDataReadInterface, metadataWriteInterface, dataTypeProvider,
        structureInterface, null, null, identifierConstraints, errorHandler, dslContextFactory, dbBackend);
  }

}
