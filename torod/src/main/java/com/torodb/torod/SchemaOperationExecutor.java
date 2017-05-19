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

import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.torod.exception.AlreadyExistentCollectionException;
import com.torodb.torod.exception.UnexistentCollectionException;
import com.torodb.torod.exception.UnexistentDatabaseException;
import com.torodb.torod.exception.UserSchemaException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 *
 */
public interface SchemaOperationExecutor extends AutoCloseable {

  /**
   * Returns a stream with the names of all databases.
   */
  public Stream<String> streamDbNames();

  /**
   * Extends the schema so all given documents fit on them.
   *
   * <p>A document <em>fits</em> in a collection if it can be stored there without schema
   * modifications, so all doc parts, scalar and fields required to store the document are declared
   * on the collection.
   *
   * <p>Once this method is returns, the backend is in a state where the given documents can be
   * inserted without changes on the schema/DDL.
   *
   * @param dbName  The name of the database. It is created if there is no database with that name
   * @param colName The name of the collection. It is created if there is no collection on the
   *                database that has that name
   * @param docs    A set with the documents that will be fit on the collection.
   * @return true iff the schema was modified (otherwise the schema was compatible with the provided
   *         argumens)
   * @throws UserSchemaException if there is a input problem while modifying the schema, like
   *                             unsupported types
   */
  public boolean prepareSchema(String dbName, String colName, Collection<KvDocument> docs)
      throws UserSchemaException;

  /**
   * Like {@link #prepareSchema(java.lang.String, java.lang.String, java.util.Collection) }, but
   * accepts a single document instead of a collection of them.
   */
  public default boolean prepareSchema(String dbName, String colName, KvDocument doc)
      throws UserSchemaException {
    return prepareSchema(dbName, colName, Collections.singleton(doc));
  }

  /**
   * Like to call {@link #prepareSchema(java.lang.String, java.lang.String, java.util.Collection) }
   * with an empty collection.
   */
  public default boolean prepareSchema(String dbName, String colName) throws UserSchemaException {
    return prepareSchema(dbName, colName, Collections.emptyList());
  }

  /**
   * Disables the data import mode on the given database.
   *
   * <p>If the database was not on import mode, nothing is done.
   *
   * @param dbName The database on which the import mode will be disabled.
   * @throws UnexistentDatabaseException if the origin database does not exist
   */
  public void disableDataImportMode(String dbName) throws UnexistentDatabaseException;

  /**
   * Enables the data import mode on the given database.
   *
   * <p>If the database was already on import mode, nothing is done.
   *
   * @param dbName The database on which the import mode will be enabled.
   * @throws UnexistentDatabaseException if the origin database does not exist
   */
  public void enableDataImportMode(String dbName) throws UnexistentDatabaseException;

  /**
   * Renames a collection.
   *
   * @param fromDb  The name of the database that should contain the collection to be renamed.
   * @param fromCol The name of the collection to be renamed.
   * @param toDb    The name of the database that will contain the renamed collection. It it doesn't
   *                exist it is automatically created.
   * @param toCol   The new name of the collection.
   *
   * @throws UnexistentDatabaseException if the origin database does not exist
   * @throws UnexistentCollectionException if the origin collection does not exist
   * @throws AlreadyExistentCollectionException if the target collection already exist
   */
  public void renameCollection(String fromDb, String fromCol, String toDb, String toCol)
      throws UnexistentDatabaseException, UnexistentCollectionException,
      AlreadyExistentCollectionException, UserException;

  /**
   * Creates the collection with the given name if it doesn't exist yet.
   *
   * <p>If the backend already contains a collection with that name on the given database, then
   * nothing is done.
   *
   * @param dbName  The name of the that will contain the collection.
   * @param colName The name of the collection to be created
   * @throws UnexistentDatabaseException if the database does not exist
   *
   */
  void createCollection(String dbName, String colName)
      throws RollbackException, UnexistentDatabaseException;


  /**
   * Drops the collection with the given name if it doesn't exist yet.
   *
   * @param dbName  The name of the database that should contain the collection.
   * @param colName The name of the database to be drop.
   * @throws UnexistentDatabaseException if the database does not exist
   *
   */
  public void dropCollection(String dbName, String colName) throws UnexistentDatabaseException;

  /**
   * Creates a database with a given name if it doesn't exist yet.
   *
   * <p>If the backend already contains a database with that name, then nothing is done.
   *
   * @param dbName The name of the database that will be created
   *
   */
  public void createDatabase(String dbName) throws RollbackException, UserException;

  /**
   * Drops the database with the given name if it does exist.
   *
   * <p>If the backend dones't contain a database with that name, then nothing is done.
   *
   * @param dbName The name of the database to be drop.
   *
   */
  void dropDatabase(String dbName) throws RollbackException;

  /**
   * Creates an index.
   *
   * <p>If the index already exist, then nothing is done.
   *
   * @param dbName    The name of the database on which the index will be created.
   * @param colName   The name of the collection on which the index will be created.
   * @param indexName The name of the index to be created
   * @param fields    The field info of the index.
   * @param unique    If it must be unique or not
   * @return true iff the index was created (otherwise there was a compatible index before)
   * @throws UnexistentDatabaseException   if the database does not exist
   * @throws UnexistentCollectionException if the collection does not exist
   * @throws UserException                 if the index is not supported
   *
   */
  public boolean createIndex(String dbName, String colName, String indexName,
      List<IndexFieldInfo> fields, boolean unique)
      throws UnexistentDatabaseException, UnexistentCollectionException, UserException;


  /**
   * Drops an index.
   *
   * <p>If there is no index with that name, nothing is done.
   *
   * @param dbName    The name of the database that should contain the index.
   * @param colName   The name of the collection that should contain the index.
   * @param indexName The name of the index to be drop.
   *
   * @returns true iff the index is dropped (otherwise the index didn't exist)
   * @throws UnexistentDatabaseException   if the database does not exist
   * @throws UnexistentCollectionException if the collection does not exist
   *
   */
  public boolean dropIndex(String dbName, String colName,
      String indexName) throws UnexistentDatabaseException, UnexistentCollectionException;

  @Override
  void close();

  boolean isClosed();

  public Stream<IndexInfo> getIndexesInfo(String dbName, String colName);

}
