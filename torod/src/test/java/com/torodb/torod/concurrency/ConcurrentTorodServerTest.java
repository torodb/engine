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

import com.torodb.torod.ProtectedServer;
import com.torodb.torod.TorodServer;
import com.torodb.torod.TorodServerContract;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;




@RunWith(JUnitPlatform.class)
public class ConcurrentTorodServerTest extends TorodServerContract {

  @Override
  protected TorodServer createServer(ProtectedServer decorated) {
    return new ConcurrentTorodServer(
        (Runnable runnable) -> new Thread(runnable),
        decorated,
        new MonitorConcurrentServerGuard(true)
    );
  }

  @Override
  protected long openSchemaHandlerMillisTimeout() {
    return 500;
  }

  @Override
  protected long openTransactionMillisTimeout() {
    return 500;
  }
}