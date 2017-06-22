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

import com.torodb.core.bundle.BundleConfig;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.repl.ReplCoreBundle;
import com.torodb.mongodb.repl.TestReplEssentialOverrideModule;
import com.torodb.mongodb.repl.commands.ReplCommandsBuilder;
import com.torodb.mongodb.repl.filters.ReplicationFilters;
import com.torodb.mongodb.repl.filters.SimpleReplicationFilters;
import com.torodb.mongodb.repl.filters.ToroDbReplicationFilters;
import com.torodb.mongodb.repl.guice.ReplEssentialOverrideModule;
import com.torodb.mongodb.repl.oplogreplier.DefaultOplogApplier;
import com.torodb.mongodb.repl.oplogreplier.DefaultOplogApplierBundle;
import com.torodb.mongodb.repl.oplogreplier.DefaultOplogApplierBundleConfig;
import com.torodb.mongodb.repl.oplogreplier.config.BufferOffHeapConfig;
import com.torodb.mongodb.repl.oplogreplier.config.BufferRollCycle;

/**
 * A utility class used to creates instances of {@link DefaultOplogApplier} configured to be run on
 * test environments.
 */
public class DefaultOplogApplierTestFactory {

  private DefaultOplogApplierTestFactory() {
  }

  /**
   * Creates a {@link DefaultOplogApplier} with the given information.
   */
  public static DefaultOplogApplierBundle createBundle(BundleConfig generalConfig,
      ReplCoreBundle replCoreBundle, MongoDbCoreBundle mongoDbCoreBundle,
      ToroDbReplicationFilters replFilters, BufferOffHeapConfig bufferOffHeapConfig) {

    ReplEssentialOverrideModule essentialOverrideModule = new TestReplEssentialOverrideModule(
        generalConfig.getEssentialInjector()
    );

    ReplCommandsBuilder testReplCommandsUtil = new ReplCommandsBuilder(
        generalConfig,
        replFilters,
        essentialOverrideModule
    );

    return new DefaultOplogApplierBundle(new DefaultOplogApplierBundleConfig(
        replCoreBundle,
        mongoDbCoreBundle,
        testReplCommandsUtil.getReplCommandsLibrary(),
        testReplCommandsUtil.getReplCommandsExecutor(),
        essentialOverrideModule,
        generalConfig, bufferOffHeapConfig)
    );
  }

  /**
   * Like {@link #createBundle(com.torodb.core.bundle.BundleConfig, com.torodb.mongodb.repl.ReplCoreBundle,
   * com.torodb.mongodb.core.MongoDbCoreBundle, com.torodb.mongodb.repl.filters.ToroDbReplicationFilters,
   * com.torodb.mongodb.repl.oplogreplier.config.BufferOffHeapConfig)}, but uses a replication
   * filter that ignores the database called <em>ignoredDb</em> and all collections called
   * <em>ignoredCol</em>
   */
  public static DefaultOplogApplierBundle createBundle(BundleConfig generalConfig,
      ReplCoreBundle replCoreBundle, MongoDbCoreBundle mongoDbCoreBundle) {
    return createBundle(
        generalConfig,
        replCoreBundle,
        mongoDbCoreBundle,
        createTestReplicationFilters(),
        createTestBufferOffHeapConfig()
    );
  }

  private static ToroDbReplicationFilters createTestReplicationFilters() {
    ReplicationFilters userFilters = new SimpleReplicationFilters() {
      @Override
      public boolean filterDatabase(String db) {
        return !"ignoredDb".equals(db);
      }

      @Override
      public boolean filterNamespace(String db, String col) {
        return filterDatabase(db) && !"ignoredCol".equals(col);
      }

      @Override
      public boolean filterIndex(IndexOptions idx) {
        return filterNamespace(idx.getDatabase(), idx.getCollection());
      }
    };
    return new ToroDbReplicationFilters(userFilters);
  }

  private static BufferOffHeapConfig createTestBufferOffHeapConfig() {
    BufferOffHeapConfig offHeapConfig = new BufferOffHeapConfig() {
      @Override
      public Boolean getEnabled() {
        return true;
      }

      @Override
      public String getPath() {
        return "/tmp/";
      }

      @Override
      public int getMaxSize() {
        return 10024;
      }

      @Override
      public BufferRollCycle getRollCycle() {
        return BufferRollCycle.HOURLY;
      }
    };
    return offHeapConfig;
  }

}
