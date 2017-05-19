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

package com.torodb.torod.impl.memory;

import com.google.inject.Singleton;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.torod.DocTransaction;
import com.torodb.torod.ProtectedServer;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.WriteDocTransaction;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

@Singleton
class MemoryTorodServer extends IdleTorodbService implements ProtectedServer {

  private final MemoryData data = new MemoryData();

  @Inject
  public MemoryTorodServer(ThreadFactory threadFactory) {
    super(threadFactory);
  }

  @Override
  public DocTransaction openReadTransaction(long timeout, TimeUnit unit) {
    return new MemoryReadOnlyTransaction(this);
  }

  @Override
  public WriteDocTransaction openWriteTransaction(long timeout, TimeUnit unit) {
    return new MemoryWriteTransaction(this);
  }

  @Override
  public SchemaOperationExecutor openSchemaOperationExecutor(long timeout, TimeUnit unit) {
    return new MemorySchemaOperationExecutor(this);
  }

  @Override
  protected void startUp() throws Exception {
  }

  @Override
  protected void shutDown() throws Exception {
    try (MemoryData.MdWriteTransaction trans = data.openWriteTransaction()) {
      trans.clear();
    }
  }

  MemoryData getData() {
    return data;
  }

}
