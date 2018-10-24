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

import com.google.inject.PrivateModule;
import com.torodb.backend.DslContextFactory;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.ddl.DdlOps;

import java.util.function.Consumer;

public abstract class BackendTestContextFactory {

  public abstract BackendTestContext<?> get();
  
  protected static void exposeTestInstances(Consumer<Class<?>> exposer) {
    exposer.accept(SqlInterface.class);
    exposer.accept(DdlOps.class);
    exposer.accept(DslContextFactory.class);
  }

  public static PrivateModule getTestModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        exposeTestInstances(clazz -> expose(clazz));
      }
    };
  }
}
