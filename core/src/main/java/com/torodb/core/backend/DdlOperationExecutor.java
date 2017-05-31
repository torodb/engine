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

import com.torodb.core.exceptions.InvalidDatabaseException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;

import java.util.stream.Stream;

/**
 * This interface is designed to be used when the DDL is required to be modified.
 *
 * <p>All methods on this annotated with {@link ExclusiveDdl} have to be executed called on
 * exclusive mode, which means that there is no {@link DmlTransaction} open at the same time. It is
 * the caller responsability to honor this precondition, so implementations do not have to enforce
 * it.
 */
public interface DdlOperationExecutor extends AutoCloseable {
  /**
   * Adds a new database.
   *
   * @param db the database to add.
   * @throws RollbackException
   */
  public void addDatabase(MetaDatabase db) throws RollbackException;

  /**
   * Adds a collection to a database.
   *
   * @param db     the database where the collection will be added. It must not have been added
   *               before.
   * @param newCol the collection to add
   * @throws RollbackException
   */
  public void addCollection(MetaDatabase db, MetaCollection newCol) throws RollbackException;

  /**
   * Adds a docPart without scalar and fields to a collection.
   *
   * Contained {@link MetaDocPart#streamFields() fields} and
   * {@link MetaDocPart#streamScalars() () scalars} are automatically added if indicated.
   *
   * @param db         the database that contains the given collection. It must have been added
   *                   before.
   * @param col        the collection where the doc part will be added. It must have been added
   *                   before
   * @param newDocPart the docPart to add
   * @param addColumns true iff fields and scalars contained by the doc part have to be
   *                   automatically added to the created doc part
   * @throws RollbackException
   */
  public void addDocPart(MetaDatabase db, MetaCollection col, MutableMetaDocPart newDocPart,
      boolean addColumns) throws RollbackException, UserException;

  public void addColumns(MetaDatabase db, MetaCollection col, MutableMetaDocPart docPart,
      Stream<? extends MetaScalar> scalars, Stream<? extends MetaField> fields)
      throws UserException, RollbackException;

  /**
   * Returns a {@link ImmutableMetaSnapshot} created from the metada stored on the backend.
   * 
   * @return the metadata stored on the database in a {@link ImmutableMetaSnapshot}.
   */
  @ExclusiveDdl
  public MetaSnapshot readMetadata();

  /**
   * Disables the data import mode, setting the normal one.
   *
   * <p>This method can be quite slow, as it is usual to execute quite expensive low level task like
   * recreate indexes.
   */
  @ExclusiveDdl
  public void disableDataImportMode(MetaDatabase db) throws RollbackException;

  /**
   * Sets the backend on a state where inserts are faster.
   *
   * <p/> During this state, only metadata operations and inserts are supported (but it is not
   * mandatory to throw an exception if other operations are recived). It is expected that each
   * call to this method is follow by a call to
   * {@link #enableDataImportMode(com.torodb.core.transaction.metainf.MetaSnapshot,
   * com.torodb.core.transaction.metainf.MetaDatabase) },
   * which will enable the default mode.
   */
  @ExclusiveDdl
  public void enableDataImportMode(MetaDatabase db) throws RollbackException;

  /**
   * Drop an existing collection.
   *
   * @param db   the database that contains the collection to drop.
   * @param col the collection to drop.
   */
  @ExclusiveDdl
  public void dropCollection(MetaDatabase db, MetaCollection col) throws RollbackException;

  /**
   * Drop an existing database.
   *
   * @param db the database to drop.
   */
  @ExclusiveDdl
  public void dropDatabase(MetaDatabase db) throws RollbackException;

  /**
   * Create a logical index on doc part.
   *
   * <p>If not yet existing, a physical index will be created for each existent and future doc part
   * fields and scalars that satisfy logical index definition.
   * @throws UserException if the index is not supported.
   */
  @ExclusiveDdl
  public void createIndex(MetaDatabase db, MutableMetaCollection col, MetaIndex index)
      throws UserException;

  /**
   * Drop a logical index on doc part. 
   * 
   * <p>Physical indexes that satisfy logical index definition and are not used by other logical
   * indexes will be dropped.
   */
  @ExclusiveDdl
  public void dropIndex(MetaDatabase db, MutableMetaCollection col, MetaIndex index);

  /**
   * Rename an existing collection.
   *
   * For historical reasons, it also copies childrean meta elements from the original collection
   * to the new one. This will be removed on future versions
   *
   * @param fromDb   the database that contains the collection to rename.
   * @param fromColl the collection to rename.
   * @param toDb     the database that will contain the renamed collection.
   * @param toColl   the renamed collection.
   */
  @ExclusiveDdl
  void renameCollection(MetaDatabase fromDb, MetaCollection fromColl,
      MutableMetaDatabase toDb, MutableMetaCollection toColl) throws RollbackException;

  /**
   * Drops all torodb elements from the backend, including metatables and their content.
   *
   * <p>After calling this method, ToroDB cannot use the underlying backend until metadata is
   * created again.
   */
  @ExclusiveDdl
  void dropAll() throws RollbackException;

  /**
   * Drops all user elements from the backend, including metatables content but not metatables.
   *
   * <p>After calling this method, ToroDB sees the underlying backend as a fresh system, similar to
   * the one that is present the first time ToroDB starts.
   */
  @ExclusiveDdl
  void dropUserData() throws RollbackException;

  @ExclusiveDdl
  void checkOrCreateMetaDataTables() throws InvalidDatabaseException;

  boolean isClosed();

  void close();
}