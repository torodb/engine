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

package com.torodb.core.backend;

import com.torodb.core.services.TorodbService;

/**
 * A service that controls the backend, offering a relational abstraction layer.
 */
public interface BackendService extends TorodbService {

  /**
   * Creates a read only DML transaction.
   */
  public DmlTransaction openReadTransaction();

  /**
   * Creates a write DML transaction.
   */
  public WriteDmlTransaction openWriteTransaction();

  /**
   * Returns the DDL operation executor, whose operations should be executed on exclusive mode,
   * which means that there are no other open {@link DmlTransaction} at the same time.
   *
   * <p>It is the caller responsability to enforce the exclusive property.
   */
  public DdlOperationExecutor openDdlOperationExecutor();
}
