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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.torodb.core.bundle.Bundle;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.bundle.DependenciesBundle;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.repl.ReplCoreBundle;
import com.torodb.mongodb.repl.ReplCoreBundleTest;
import com.torodb.mongodb.repl.TestBundleConfig;
import com.torodb.mongodb.repl.oplogreplier.DefaultOplogApplierBundle;
import com.torodb.mongodb.repl.oplogreplier.OplogApplier;

import java.util.List;
import java.util.function.Function;

/**
 *
 */
public class DefaultOplogApplierBundleFactory
    implements Function<MongoDbCoreBundle, Bundle<OplogApplier>> {

  @Override
  public Bundle<OplogApplier> apply(MongoDbCoreBundle coreBundle) {
    return new MetaBundle(new TestBundleConfig(), coreBundle);
  }

  /**
   * A bundle that returns the {@link OplogApplier} to be tested, but also controls the lifecycle of
   * the {@link ReplCoreBundle} and {@link DefaultOplogApplierBundle} it depends on.
   */
  private static class MetaBundle extends DependenciesBundle<OplogApplier> {

    private final MongoDbCoreBundle coreBundle;
    private final ReplCoreBundle replCoreBundle;
    private final DefaultOplogApplierBundle oplogApplierBundle;

    public MetaBundle(BundleConfig bundleConfig, MongoDbCoreBundle coreBundle) {
      super(bundleConfig);

      this.coreBundle = coreBundle;

      replCoreBundle = ReplCoreBundleTest.createBundle(bundleConfig, coreBundle);

      oplogApplierBundle = DefaultOplogApplierTestFactory.createBundle(
          bundleConfig,
          replCoreBundle,
          coreBundle
      );

    }

    @Override
    protected List<Service> getManagedDependencies() {
      return Lists.newArrayList(
          replCoreBundle,
          oplogApplierBundle
      );
    }

    @Override
    protected void postDependenciesStartUp() throws Exception {
      coreBundle.awaitRunning();
      super.postDependenciesStartUp();
    }

    @Override
    public OplogApplier getExternalInterface() {
      return oplogApplierBundle.getExternalInterface().getOplogApplier();
    }


  }
}
