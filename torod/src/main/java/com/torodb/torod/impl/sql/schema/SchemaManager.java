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


package com.torodb.torod.impl.sql.schema;

import com.torodb.common.util.Empty;
import com.torodb.core.backend.DdlOperationExecutor;
import com.torodb.core.services.TorodbService;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.exception.UnexistentCollectionException;
import com.torodb.torod.exception.UnexistentDatabaseException;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Implementations of this interface are designed to deal with changes on the SQL schema on
 * an async way.
 */
public interface SchemaManager extends TorodbService {

  /**
   * Overrides the metadata information with the metainformation found on the backend.
   * @param ops The backend object used to get the database schema.
   */
  CompletableFuture<Empty> refreshMetadata(DdlOperationExecutor ops);

  /**
   * Extends the schema so all given documents fit on them.
   *
   * <p>A document <em>fits</em> in a collection if it can be stored there without schema
   * modifications, so all doc parts, scalar and fields required to store the document are declared
   * on the collection.
   *
   * <p>Once the returned future is done, the backend is in a state where the given documents can be
   * inserted without changes on the schema/DDL.
   *
   * @param ops        The backend object used to modify the database schema.
   * @param db         The name of the database. It is created if there is no database with that
   *                   name
   * @param collection The name of the collection. It is created if there is no collection on the
   *                   database that has that name
   * @param docs       A set with the documents that will be fit on the collection.
   */
  CompletableFuture<Boolean> prepareSchema(DdlOperationExecutor ops, String db,
      String collection, Collection<KvDocument> docs);

  /**
   * Creates a database with a given name if it doesn't exist yet.
   *
   * <p>If the backend already contains a database with that name, then nothing is done.
   *
   * @param ops    The backend object used to modify the database schema.
   * @param dbName The name of the database that will be created. It must exist.
   * 
   */
  CompletableFuture<Empty> createDatabase(DdlOperationExecutor ops, String dbName);

  /**
   * Drops the database with the given name if it does exist.
   *
   * <p>If the backend dones't contain a database with that name, then nothing is done.
   *
   * @param ops    The backend object used to modify the database schema.
   * @param dbName The name of the database to be drop.
   * 
   */
  CompletableFuture<Empty> dropDatabase(DdlOperationExecutor ops, String dbName);

  /**
   * Creates the collection with the given name if it doesn't exist yet.
   *
   * <p>If the backend already contains a collection with that name on the given database, then
   * nothing is done. 
   *
   * @param ops     The backend object used to modify the database schema.
   * @param dbName  The name of the that will contain the collection. It msut exist.
   * @param colName The name of the collection to be created
   * @throws UnexistentDatabaseException if the database does not exist
   * 
   */
  CompletableFuture<Empty> createCollection(DdlOperationExecutor ops, String dbName,
      String colName);

  /**
   * Drops the collection with the given name if it doesn't exist yet.
   *
   * @param ops     The backend object used to modify the database schema.
   * @param dbName  The name of the database that should contain the collection. It must exist.
   * @param colName The name of the database to be drop.
   * @throws UnexistentDatabaseException if the database does not exist
   * 
   */
  CompletableFuture<Empty> dropCollection(DdlOperationExecutor ops, String dbName, String colName);

  /**
   * Renames a collection.
   *
   * @param ops     The backend object used to modify the database schema.
   * @param fromDb  The name of the database that should contain the collection to be renamed. It
   *                must exist.
   * @param fromCol The name of the collection to be renamed. It must exist
   * @param toDb    The name of the database that will contain the renamed collection. It it doesn't
   *                exist it is automatically created.
   * @param toCol   The new name of the collection. It cannot be already created.
   */
  CompletableFuture<Empty> renameCollection(DdlOperationExecutor ops, String fromDb, String fromCol,
      String toDb, String toCol);

  /**
   * Disables the data import mode on the given database.
   *
   * <p>If the database was not on import mode, nothing is done.
   *
   * @param ops    The backend object used to modify the database schema.
   * @param dbName The database on which the import mode will be disabled. It must exist.
   * @throws UnexistentDatabaseException if the origin database does not exist
   */
  CompletableFuture<Empty> disableDataImportMode(DdlOperationExecutor ops, String dbName);

  /**
   * Enables the data import mode on the given database.
   *
   * <p>If the database was already on import mode, nothing is done.
   *
   * @param ops    The backend object used to modify the database schema.
   * @param dbName The database on which the import mode will be enabled. It must exist.
   * @throws UnexistentDatabaseException if the origin database does not exist
   */
  CompletableFuture<Empty> enableDataImportMode(DdlOperationExecutor ops, String dbName);

  /**
   * Creates an index.
   *
   * <p>If the index already exist, then nothing is done.
   *
   * @param ops       The backend object used to modify the database schema.
   * @param dbName    The name of the database on which the index will be created. It must exist.
   * @param colName   The name of the collection on which the index will be created. It must exist.
   * @param indexName The name of the index to be created
   * @param fields    The field info of the index.
   * @param unique    If it must be unique or not
   * @throws UnexistentDatabaseException if the database does not exist
   * @throws UnexistentCollectionException if the collection does not exist
   * 
   */
  CompletableFuture<Boolean> createIndex(DdlOperationExecutor ops, String dbName, String colName,
      String indexName, List<IndexFieldInfo> fields, boolean unique);

  /**
   * Drops an index.
   *
   * <p>If there is no index with that name, nothing is done.
   *
   * @param ops       The backend object used to modify the database schema.
   * @param dbName    The name of the database that should contain the index. It must exist.
   * @param colName   The name of the collection that should contain the index. It must exist.
   * @param indexName The name of the index to be drop.
   *
   */
  CompletableFuture<Boolean> dropIndex(DdlOperationExecutor ops, String dbName, String colName,
      String indexName);

  /**
   * Returns an up-to-date immutable view of the metainformation.
   *
   */
  CompletableFuture<ImmutableMetaSnapshot> getMetaSnapshot();

  /**
   * Executes the given function atomically.
   *
   * It is guaranteed that the schema will not change meanwhile this method is called.
   * @param <E> The type of the returned function
   * @param fun A function that given a snapshot, returns a value
   * @return the value returned by the function
   */
  <E> CompletableFuture<E> executeAtomically(Function<ImmutableMetaSnapshot, E> fun);

}