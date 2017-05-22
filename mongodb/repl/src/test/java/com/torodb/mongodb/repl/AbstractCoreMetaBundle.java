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

import com.google.common.util.concurrent.Service;
import com.torodb.core.bundle.AbstractBundle;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.logging.DefaultLoggerFactory;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.core.MongoDbCoreConfig;
import com.torodb.torod.TorodBundle;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;


public class AbstractCoreMetaBundle extends AbstractBundle<MongoDbCoreBundle> {

  private final TorodBundle torodBundle;
  private final MongoDbCoreBundle mongoDbCoreBundle;

  public AbstractCoreMetaBundle(BundleConfig generalConfig, TorodBundle torodBundle) {
    super(generalConfig);

    this.torodBundle = torodBundle;

    MongoDbCoreConfig mongoDbCoreConfig = MongoDbCoreConfig.simpleNonServerConfig(
        torodBundle,
        DefaultLoggerFactory.getInstance(),
        Optional.empty(),
        generalConfig);

    mongoDbCoreBundle = new MongoDbCoreBundle(mongoDbCoreConfig);
  }

  @Override
  public Collection<Service> getDependencies() {
    return Collections.emptyList();
  }

  @Override
  public MongoDbCoreBundle getExternalInterface() {
    return mongoDbCoreBundle;
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
    torodBundle.startAsync();
    torodBundle.awaitRunning();

    mongoDbCoreBundle.startAsync();
    mongoDbCoreBundle.awaitRunning();
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    mongoDbCoreBundle.stopAsync();
    mongoDbCoreBundle.awaitTerminated();

    torodBundle.stopAsync();
    torodBundle.awaitTerminated();
  }

}
