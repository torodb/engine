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

import com.torodb.core.services.TorodbService;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 *
 */
public interface TorodServer extends TorodbService {

  public DocTransaction openReadTransaction(long timeout, TimeUnit unit) throws TimeoutException;

  /**
   * Like {@link #openReadTransaction(long, java.util.concurrent.TimeUnit)}, but uses a default
   * timeout.
   */
  public DocTransaction openReadTransaction() throws TimeoutException;

  public WriteDocTransaction openWriteTransaction(long timeout, TimeUnit unit) throws
      TimeoutException;

  /**
   * Like {@link #openWriteTransaction(long, java.util.concurrent.TimeUnit) }, but uses a default
   * timeout.
   */
  public WriteDocTransaction openWriteTransaction() throws TimeoutException;

  public SchemaOperationExecutor openSchemaOperationExecutor(long timeout, TimeUnit unit) throws
      TimeoutException;

  /**
   * Like {@link #openSchemaOperationExecutor(long, java.util.concurrent.TimeUnit) }, but uses a
   * default timeout.
   */
  public SchemaOperationExecutor openSchemaOperationExecutor() throws TimeoutException;

}
