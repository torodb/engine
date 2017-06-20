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

package com.torodb.mongodb.repl.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.supervision.Supervisor;

public class FollowerSyncSourceProviderConfig implements BundleConfig {
  private final ImmutableList<HostAndPort> seeds;
  private final BundleConfig delegate;

  public FollowerSyncSourceProviderConfig(ImmutableList<HostAndPort> seeds, BundleConfig delegate) {
    this.seeds = seeds;
    this.delegate = delegate;
  }

  public ImmutableList<HostAndPort> getSeeds() {
    return seeds;
  }

  @Override
  public Injector getEssentialInjector() {
    return delegate.getEssentialInjector();
  }

  @Override
  public Supervisor getSupervisor() {
    return delegate.getSupervisor();
  }
}
