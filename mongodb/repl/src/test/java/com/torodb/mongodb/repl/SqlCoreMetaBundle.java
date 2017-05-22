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

package com.torodb.mongodb.repl;

import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.BackendService;
import com.torodb.core.backend.DdlOperationExecutor;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.impl.sql.SqlTorodBundle;
import com.torodb.torod.impl.sql.SqlTorodConfig;


public class SqlCoreMetaBundle extends AbstractCoreMetaBundle {

  private final BackendBundle backendBundle;

  /**
   *
   * @param backendBundle it will be managed by the newly created object
   */
  public SqlCoreMetaBundle(BackendBundle backendBundle) {
    this(new TestBundleConfig(), backendBundle);
  }

  /**
   *
   * @param generalConfig
   * @param backendBundle it will be managed by the newly created object
   */
  public SqlCoreMetaBundle(BundleConfig generalConfig, BackendBundle backendBundle) {
    super(generalConfig, createTorodBundle(generalConfig, backendBundle));
    this.backendBundle = backendBundle;
  }

  public BackendBundle getBackendBundle() {
    return backendBundle;
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    super.preDependenciesShutDown();

    BackendService backendService = backendBundle.getExternalInterface().getBackendService();

    try (DdlOperationExecutor ddlEx = backendService.openDdlOperationExecutor()) {
      //try to drop everything just in case another test uses the same backend
      ddlEx.dropAll();
    } finally {
      backendBundle.stopAsync();
      backendBundle.awaitTerminated();
    }
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
    backendBundle.startAsync();
    backendBundle.awaitRunning();;
    
    super.postDependenciesStartUp();
  }

  private static TorodBundle createTorodBundle(
      BundleConfig generalConfig, BackendBundle backendBundle) {

    return new SqlTorodBundle(new SqlTorodConfig(backendBundle, generalConfig));
  }

}
