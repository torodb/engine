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

package com.torodb.torod;

import com.torodb.core.bundle.AbstractBundle;
import com.torodb.core.bundle.BundleConfig;
import org.apache.logging.log4j.Logger;



public abstract class AbstractTorodBundle extends AbstractBundle<TorodExtInt> 
    implements TorodBundle {

  private static final Logger LOGGER = TorodLoggerFactory.get(AbstractTorodBundle.class);
  
  protected AbstractTorodBundle(BundleConfig config) {
    super(config);
  }

  /**
   * Returns the {@linkplain TorodServer} this bundle will use.
   *
   * <p>It must always return the same instance (or at least instances that share the service state)
   */
  protected abstract TorodServer getTorodServer();

  @Override
  protected void postDependenciesStartUp() throws Exception {
    LOGGER.debug("Starting Torod sevice");
    TorodServer torodServer = getTorodServer();
    torodServer.startAsync();
    torodServer.awaitRunning();
    LOGGER.debug("Torod sevice started");
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    TorodServer torodServer = getTorodServer();
    torodServer.stopAsync();
    torodServer.awaitTerminated();
  }

  @Override
  public TorodExtInt getExternalInterface() {
    return () -> getTorodServer();
  }
}
