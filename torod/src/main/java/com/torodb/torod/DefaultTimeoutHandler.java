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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 */
public class DefaultTimeoutHandler {

  private final TorodServer server;
  private final long readDefaultTimeoutMillis;
  private final long writeDefaultTimeoutMillis;
  private final long schemaDefaultTimeoutMillis;

  public DefaultTimeoutHandler(TorodServer server, Duration readDefaultTimeout,
      Duration writeDefaultTimeout, Duration schemaDefaultTimeout) {
    this.server = server;
    this.readDefaultTimeoutMillis = readDefaultTimeout.toMillis();
    this.writeDefaultTimeoutMillis = writeDefaultTimeout.toMillis();
    this.schemaDefaultTimeoutMillis = schemaDefaultTimeout.toMillis();
  }

  public DefaultTimeoutHandler(TorodServer server, Duration defaultTimeout) {
    this(server, defaultTimeout, defaultTimeout);
  }

  public DefaultTimeoutHandler(TorodServer server, Duration defaultTransTimeout,
      Duration defaultSchemaTimeout) {
    this(server, defaultTransTimeout, defaultTransTimeout, defaultSchemaTimeout);
  }

  public DocTransaction openReadTransaction() throws TimeoutException {
    return server.openReadTransaction(readDefaultTimeoutMillis, TimeUnit.MILLISECONDS);
  }

  public WriteDocTransaction openWriteTransaction() throws TimeoutException {
    return server.openWriteTransaction(writeDefaultTimeoutMillis, TimeUnit.MILLISECONDS);
  }

  public SchemaOperationExecutor openSchemaOperationExecutor()
      throws TimeoutException {
    return server.openSchemaOperationExecutor(schemaDefaultTimeoutMillis, TimeUnit.MILLISECONDS);
  }
}