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

package com.torodb.torod.concurrency;

import com.google.inject.Exposed;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.torod.ProtectedServer;
import com.torodb.torod.TorodServer;

import java.util.concurrent.ThreadFactory;

import javax.inject.Singleton;

public class ConcurrentTorodModule extends PrivateModule {

  @Override
  protected void configure() {
    expose(TorodServer.class);
  }

  @Exposed
  @Provides
  @Singleton
  public TorodServer createConcurrentTorodServer(
      @TorodbIdleService ThreadFactory threadFactory, ProtectedServer decorated) {
    ConcurrentServerGuard guard = new MonitorConcurrentServerGuard(true);
    return new ConcurrentTorodServer(threadFactory, decorated, guard);
  }

}