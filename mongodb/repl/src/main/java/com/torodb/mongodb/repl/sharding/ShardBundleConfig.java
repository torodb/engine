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

package com.torodb.mongodb.repl.sharding;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import com.torodb.core.bundle.BundleConfigImpl;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.repl.ConsistencyHandler;
import com.torodb.mongodb.repl.filters.ReplicationFilters;
import com.torodb.mongowp.client.wrapper.MongoClientConfigurationProperties;
import com.torodb.torod.TorodBundle;

public class ShardBundleConfig extends BundleConfigImpl {
  private final String shardId;
  private final TorodBundle torodBundle;
  private final ImmutableList<HostAndPort> seeds;
  private final MongoClientConfigurationProperties clientConfigProperties;
  private final String replSetName;
  private final ReplicationFilters userReplFilter;
  private final ConsistencyHandler consistencyHandler;
  private final LoggerFactory lifecycleLoggingFactory;

  public ShardBundleConfig(String shardId, TorodBundle torodBundle,
      ImmutableList<HostAndPort> seeds,
      MongoClientConfigurationProperties clientConfigProperties, 
      String replSetName, ReplicationFilters userReplFilter,
      ConsistencyHandler consistencyHandler, LoggerFactory lifecycleLoggingFactory,
      Injector essentialInjector, Supervisor supervisor) {
    super(essentialInjector, supervisor);
    this.shardId = shardId;
    this.torodBundle = torodBundle;
    this.seeds = seeds;
    this.clientConfigProperties = clientConfigProperties;
    this.replSetName = replSetName;
    this.userReplFilter = userReplFilter;
    this.consistencyHandler = consistencyHandler;
    this.lifecycleLoggingFactory = lifecycleLoggingFactory;
  }

  /**
   * The id of the shard that is going to be replicated.
   *
   * Two {@link ShardBundle} can be running concurrently on the same backend if they compatible
   * between them and their shard id is different.
   */
  public String getShardId() {
    return shardId;
  }

  public TorodBundle getTorodBundle() {
    return torodBundle;
  }

  public ImmutableList<HostAndPort> getSeeds() {
    return seeds;
  }

  public MongoClientConfigurationProperties getClientConfigProperties() {
    return clientConfigProperties;
  }

  public ConsistencyHandler getConsistencyHandler() {
    return consistencyHandler;
  }

  public String getReplSetName() {
    return replSetName;
  }

  public ReplicationFilters getUserReplFilter() {
    return userReplFilter;
  }

  public LoggerFactory getLifecycleLoggingFactory() {
    return lifecycleLoggingFactory;
  }


}
