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

import com.torodb.core.d2r.DocPartData;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.kvdocument.values.KvValue;

import java.util.Collection;

public interface WriteDmlTransaction extends DmlTransaction {
  
  /**
   * Reserves a given number of rids on the given doc part.
   *
   * @param db      the database that contains the given collection
   * @param col     the collection that contains the given doc part
   * @param docPart the doc part where rid want to be consumed
   * @param howMany how many rids want to be consumed.
   * @return the first rid that can be used.
   */
  public int consumeRids(MetaDatabase db, MetaCollection col, MetaDocPart docPart, int howMany)
      throws RollbackException;

  /**
   *
   * @param db   the database that contains the given collection
   * @param col  the collection that contains the given data
   * @param data the rows to be inserted
   * @throws UserException if there is a bussiness problem with the operation to be inserted (like a
   *                       unique index violation)
   */
  public void insert(MetaDatabase db, MetaCollection col, DocPartData data) 
      throws RollbackException, UserException;

  public void deleteDids(MetaDatabase db, MetaCollection col, Collection<Integer> dids);

  /**
   * Stores the given key value association.
   *
   * <p>This metainfo is a key-value storage that different modules can use to store their own
   * information.
   *
   * @param key the key of the value to be written
   * @param newValue the value to be written
   * @return the old value or null if none was stored
   * @throws IllegalArgumentException if the given key is not registered
   */
  public KvValue<?> writeMetaInfo(MetaInfoKey key, KvValue<?> newValue);

  public void commit() throws UserException, RollbackException;

  @Override
  public void close();
}