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

package com.torodb.torod.impl.sql.schema;

import com.google.inject.Exposed;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.torodb.core.TableRefFactory;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.supervision.Supervisor;

import java.util.concurrent.ThreadFactory;

import javax.inject.Singleton;

/**
 *
 */
public class SqlSchemaModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(DocSchemaAnalyzer.class);

    expose(SyncSchemaManager.class);
    bind(SyncSchemaManager.class);
  }

  @Exposed
  @Singleton
  @Provides
  SchemaManager createSchemaManager(
      @TorodbIdleService ThreadFactory tf,
      ConcurrentToolsFactory concurrentToolsFactory,
      Logic logic) {
    return new SingleThreadSchemaManager(tf, concurrentToolsFactory, logic);
  }

  @Singleton
  @Provides
  Logic createLogic(IdentifierFactory idFactory, DocSchemaAnalyzer docSchemaAnalyzer,
      TableRefFactory tableRefFactory, Supervisor supervisor) {
    return new Logic(
        idFactory,
        tableRefFactory::translate,
        docSchemaAnalyzer,
        supervisor
    );
  }
}