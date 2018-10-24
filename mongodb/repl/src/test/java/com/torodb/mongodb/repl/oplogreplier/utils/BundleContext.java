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

package com.torodb.mongodb.repl.oplogreplier.utils;

import com.torodb.core.bundle.Bundle;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.repl.oplogreplier.ApplierContext;
import com.torodb.mongodb.repl.oplogreplier.OplogApplier;
import com.torodb.mongowp.commands.oplog.OplogOperation;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 *
 */
final class BundleContext implements ClosableContext {

  private final Bundle<MongoDbCoreBundle> mongodMetaBundle;
  private final Bundle<OplogApplier> applierBundle;
  private final DefaultContext delegated;

  public BundleContext(Bundle<MongoDbCoreBundle> mongodMetaBundle,
      Function<MongoDbCoreBundle, Bundle<OplogApplier>> oplogApplierBundleFactory) {
    this.mongodMetaBundle = mongodMetaBundle;

    boolean success = false;
    try {
      mongodMetaBundle.start().join();
      MongoDbCoreBundle coreBundle = mongodMetaBundle.getExternalInterface();
      assert coreBundle.isRunning();

      applierBundle = oplogApplierBundleFactory.apply(coreBundle);
      applierBundle.start().join();

      delegated = new DefaultContext(
          coreBundle.getExternalInterface().getMongodServer(),
          applierBundle.getExternalInterface()
      );
      success = true;
    } finally {
      if (!success) { //something went wrong during the initialization
        close(); //try to close any open resource
      }
    }
  }

  @Override
  public MongodServer getMongodServer() {
    return delegated.getMongodServer();
  }

  @Override
  public void apply(Stream<OplogOperation> streamOplog, ApplierContext applierContext) throws
      Exception {
    delegated.apply(streamOplog, applierContext);
  }

  @Override
  public void close() {
    if (applierBundle != null && applierBundle.isRunning()) {
      applierBundle.stop().join();
    }
    if (mongodMetaBundle != null && mongodMetaBundle.isRunning()) {
      mongodMetaBundle.stop().join();
    }
  }

}
