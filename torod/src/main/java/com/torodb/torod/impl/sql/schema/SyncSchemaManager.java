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

import com.google.common.base.Throwables;
import com.torodb.common.util.CompletionExceptions;
import com.torodb.core.backend.DdlOperationExecutor;
import com.torodb.core.exceptions.ToroRuntimeException;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.exception.AlreadyExistentCollectionException;
import com.torodb.torod.exception.SchemaOperationException;
import com.torodb.torod.exception.UnexistentCollectionException;
import com.torodb.torod.exception.UnexistentDatabaseException;
import com.torodb.torod.exception.UserSchemaException;
import com.torodb.torod.impl.sql.schema.SchemaManager;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import javax.inject.Inject;

/**
 * This is a sync wrapper on a async {@link SchemaManager}.
 */
public class SyncSchemaManager {

  private final SchemaManager async;

  @Inject
  public SyncSchemaManager(SchemaManager async) {
    this.async = async;
  }

  /**
   * Overrides the metadata information with the metainformation found on the backend.
   * @param ops The backend object used to get the database schema.
   */
  public void refreshMetadata(DdlOperationExecutor ops) {
    waitFor(async.refreshMetadata(ops));
  }

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
   * @param ops     The backend object used to modify the database schema.
   * @param dbName  The name of the database. It is created if there is no database with that name
   * @param colName The name of the collection. It is created if there is no collection on the
   *                database that has that name
   * @param docs    A set with the documents that will be fit on the collection.
   * @throws UserSchemaException if there is a input problem while modifying the schema, like
   *                             unsupported types
   */
  public boolean prepareSchema(DdlOperationExecutor ops, String dbName, String colName,
      Collection<KvDocument> docs) throws UserSchemaException {
    return waitFor(async.prepareSchema(ops, dbName, colName, docs), UserSchemaException.class);
  }

  /**
   * Creates a database with a given name if it doesn't exist yet.
   *
   * <p>If the backend already contains a database with that name, then nothing is done.
   *
   * @param ops    The backend object used to modify the database schema.
   * @param dbName The name of the database that will be created
   * 
   */
  public void createDatabase(DdlOperationExecutor ops, String dbName) {
    waitFor(async.createDatabase(ops, dbName));
  }

  /**
   * Drops the database with the given name if it does exist.
   *
   * <p>If the backend dones't contain a database with that name, then nothing is done.
   *
   * @param ops    The backend object used to modify the database schema.
   * @param dbName The name of the database to be drop.
   * 
   */
  public void dropDatabase(DdlOperationExecutor ops, String dbName) {
    waitFor(async.dropDatabase(ops, dbName));
  }

  /**
   * Creates the collection with the given name if it doesn't exist yet.
   *
   * <p>If the backend already contains a collection with that name on the given database, then
   * nothing is done. 
   *
   * @param ops     The backend object used to modify the database schema.
   * @param dbName  The name of the that will contain the collection.
   * @param colName The name of the collection to be created
   * @throws UnexistentDatabaseException if the database does not exist
   * 
   */
  public void createCollection(DdlOperationExecutor ops, String dbName, String colName)
      throws UnexistentDatabaseException {
    waitFor(async.createCollection(ops, dbName, colName), UnexistentDatabaseException.class);
  }

  /**
   * Drops the collection with the given name if it doesn't exist yet.
   *
   * @param ops     The backend object used to modify the database schema.
   * @param dbName  The name of the database that should contain the collection.
   * @param colName The name of the database to be drop.
   * @throws UnexistentDatabaseException if the database does not exist
   * 
   */
  public void dropCollection(DdlOperationExecutor ops, String dbName, String colName)
      throws UnexistentDatabaseException {
    waitFor(async.dropCollection(ops, dbName, colName), UnexistentCollectionException.class);
  }

  /**
   * Renames a collection.
   *
   * @param ops     The backend object used to modify the database schema.
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
  public void renameCollection(DdlOperationExecutor ops, String fromDb, String fromCol,
      String toDb, String toCol) throws UnexistentDatabaseException, UnexistentCollectionException,
      AlreadyExistentCollectionException {
    waitFor(async.renameCollection(ops, fromDb, fromCol, toDb, toCol),
        UnexistentDatabaseException.class,
        UnexistentCollectionException.class,
        AlreadyExistentCollectionException.class
    );
  }

  /**
   * Disables the data import mode on the given database.
   *
   * <p>If the database was not on import mode, nothing is done.
   *
   * @param ops    The backend object used to modify the database schema.
   * @param dbName The database on which the import mode will be disabled.
   * @throws UnexistentDatabaseException if the origin database does not exist
   */
  public void disableDataImportMode(DdlOperationExecutor ops, String dbName)
      throws UnexistentDatabaseException {
    waitFor(async.disableDataImportMode(ops, dbName), UnexistentDatabaseException.class);
  }

  /**
   * Enables the data import mode on the given database.
   *
   * <p>If the database was already on import mode, nothing is done.
   *
   * @param ops    The backend object used to modify the database schema.
   * @param dbName The database on which the import mode will be enabled.
   * @throws UnexistentDatabaseException if the origin database does not exist
   */
  public void enableDataImportMode(DdlOperationExecutor ops, String dbName)
      throws UnexistentDatabaseException {
    waitFor(async.enableDataImportMode(ops, dbName), UnexistentDatabaseException.class);
  }

  /**
   * Creates an index.
   *
   * <p>If the index already exist, then nothing is done.
   *
   * @param ops       The backend object used to modify the database schema.
   * @param dbName    The name of the database on which the index will be created.
   * @param colName   The name of the collection on which the index will be created.
   * @param indexName The name of the index to be created
   * @param fields    The field info of the index.
   * @param unique    If it must be unique or not
   * @throws UnexistentDatabaseException if the database does not exist
   * @throws UnexistentCollectionException if the collection does not exist
   * 
   */
  public boolean createIndex(DdlOperationExecutor ops, String dbName, String colName,
      String indexName, List<IndexFieldInfo> fields, boolean unique)
      throws UnexistentDatabaseException, UnexistentCollectionException {
    return waitFor(async.createIndex(ops, dbName, colName, indexName, fields, unique),
        UnexistentDatabaseException.class,
        UnexistentCollectionException.class);
  }

  /**
   * Drops an index.
   *
   * <p>If there is no index with that name, nothing is done.
   *
   * @param ops       The backend object used to modify the database schema.
   * @param dbName    The name of the database that should contain the index.
   * @param colName   The name of the collection that should contain the index.
   * @param indexName The name of the index to be drop.
   *
   * @throws UnexistentDatabaseException   if the database does not exist
   * @throws UnexistentCollectionException if the collection does not exist
   *
   */
  public boolean dropIndex(DdlOperationExecutor ops, String dbName, String colName,
      String indexName) throws UnexistentDatabaseException, UnexistentCollectionException {
    return waitFor(async.dropIndex(ops, dbName, colName, indexName),
        UnexistentDatabaseException.class,
        UnexistentCollectionException.class);
  }

  /**
   * Returns an up-to-date immutable view of the metainformation.
   *
   */
  public ImmutableMetaSnapshot getMetaSnapshot() {
    return waitFor(async.getMetaSnapshot());
  }

  /**
   * Executes the given function atomically.
   *
   * It is guaranteed that the schema will not change meanwhile this method is called.
   * @param <E> The type of the returned function
   * @param fun A function that given a snapshot, returns a value
   * @return the value returned by the function
   */
  public <E> E executeAtomically(Function<ImmutableMetaSnapshot, E> fun) {
    return waitFor(async.executeAtomically(fun));
  }

  private <R> R waitFor(CompletableFuture<R> future) {
    try {
      return future.join();
    } catch (CompletionException ex) {
      Throwables.throwIfUnchecked(ex.getCause());
      throw new ToroRuntimeException("Unexpected exception",
          CompletionExceptions.getFirstNonCompletionException(ex));
    }
  }

  private <R, E1 extends SchemaOperationException> R waitFor(
      CompletableFuture<R> future, Class<E1> exClass) throws E1 {
    try {
      return future.join();
    } catch (CompletionException ex) {
      Throwable firstNonCompletion = CompletionExceptions.getFirstNonCompletionException(ex);
      Throwables.throwIfInstanceOf(firstNonCompletion, exClass);
      Throwables.throwIfInstanceOf(firstNonCompletion, RuntimeException.class);
      throw new ToroRuntimeException("Unexpected exception", firstNonCompletion);
    }
  }

  private <R, E1 extends SchemaOperationException, E2 extends SchemaOperationException>
      R waitFor(CompletableFuture<R> future, Class<E1> ex1Class, Class<E2> ex2Class)
          throws E1, E2 {
    try {
      return future.join();
    } catch (CompletionException ex) {
      Throwables.throwIfInstanceOf(ex.getCause(), ex1Class);
      Throwables.throwIfInstanceOf(ex.getCause(), ex2Class);
      throw new ToroRuntimeException("Unexpected exception",
          CompletionExceptions.getFirstNonCompletionException(ex));
    }
  }

  @SuppressWarnings("checkstyle:LineLength")
  private <R, E1 extends SchemaOperationException, E2 extends SchemaOperationException,
          E3 extends SchemaOperationException> R waitFor(
          CompletableFuture<R> future, Class<E1> ex1Class, Class<E2> ex2Class, Class<E3> ex3Class)
      throws E1, E2, E3 {
    try {
      return future.join();
    } catch (SchemaOperationException ex) {
      Throwables.throwIfInstanceOf(ex.getCause(), ex1Class);
      Throwables.throwIfInstanceOf(ex.getCause(), ex2Class);
      Throwables.throwIfInstanceOf(ex.getCause(), ex3Class);
      throw new ToroRuntimeException("Unexpected exception",
          CompletionExceptions.getFirstNonCompletionException(ex));
    }
  }
}