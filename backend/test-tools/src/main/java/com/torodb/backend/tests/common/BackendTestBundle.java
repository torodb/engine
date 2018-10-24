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
package com.torodb.backend.tests.common;

import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.torodb.backend.BackendConfig;
import com.torodb.backend.DslContextFactory;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.ddl.DdlOps;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.services.TorodbService;

public interface BackendTestBundle extends TorodbService {

  public PrivateModule getBackendModule(BackendConfig config);
  
  public BackendTestExt getExternalTestInterface();
  
  public static class BackendTestExt {

    private final SqlInterface sqlInterface;
    private final DdlOps ddlOps;
    private final TableRefFactory tableRefFactory;
    private final DslContextFactory dslContextFactory;
    private final SchemaUpdater schemaUpdater;
    private final D2RTranslatorFactory d2rTranslatorFactory;
    private final R2DTranslator r2dTranslator;
    private final ReservedIdGenerator idGenerator;

    public BackendTestExt(Injector injector) {
      super();
      sqlInterface = injector.getInstance(SqlInterface.class);
      ddlOps = injector.getInstance(DdlOps.class);
      tableRefFactory = injector.getInstance(TableRefFactory.class);
      dslContextFactory = injector.getInstance(DslContextFactory.class);
      schemaUpdater = injector.getInstance(SchemaUpdater.class);
      d2rTranslatorFactory = injector.getInstance(D2RTranslatorFactory.class);
      r2dTranslator = injector.getInstance(R2DTranslator.class);
      idGenerator = injector.getInstance(ReservedIdGenerator.class);
    }

    public SqlInterface getSqlInterface() {
      return sqlInterface;
    }

    public DdlOps getDdlOps() {
      return ddlOps;
    }

    public TableRefFactory getTableRefFactory() {
      return tableRefFactory;
    }

    public DslContextFactory getDslContextFactory() {
      return dslContextFactory;
    }

    public SchemaUpdater getSchemaUpdater() {
      return schemaUpdater;
    }

    public D2RTranslatorFactory getD2RTranslatorFactory() {
      return d2rTranslatorFactory;
    }

    public R2DTranslator getR2DTranslator() {
      return r2dTranslator;
    }

    public ReservedIdGenerator getReservedIdGenerator() {
      return idGenerator;
    }
  }

}
