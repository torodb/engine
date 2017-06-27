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

import com.google.inject.Injector;
import com.torodb.backend.BackendConfig;
import com.torodb.backend.tests.common.BackendTestBundle;
import com.torodb.core.d2r.impl.D2RModule;

public abstract class MySqlBackendTestBundle extends MySqlBackendBundle implements BackendTestBundle {

  private BackendTestExt ext;

  public MySqlBackendTestBundle(BackendConfig config) {
    super(config);
  }

  @Override
  protected Injector createInjector(BackendConfig config) {
    Injector injector = config.getEssentialInjector().createChildInjector(
        getBackendModule(config), new D2RModule());

    ext = new BackendTestExt(injector);
    
    return injector;
  }

  @Override
  public BackendTestExt getExternalTestInterface() {
    return ext;
  }

}
