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

package com.torodb.mongodb.repl.oplogreplier.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.truth.Truth;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.retrier.DefaultRetrier;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.RetrierAbortException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.kvdocument.values.utils.UnorderedDocEquals;
import com.torodb.mongodb.core.MongodSchemaExecutor;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.MongodTransaction;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.repl.oplogreplier.ApplierContext;
import com.torodb.mongowp.commands.oplog.OplogOperation;
import com.torodb.torod.DocTransaction;
import org.junit.Assert;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

public abstract class BddOplogTest implements OplogApplierTest {

  private static final Retrier RETRIER = DefaultRetrier.getInstance();

  protected abstract Collection<DatabaseState> getInitialState();

  protected abstract Collection<DatabaseState> getExpectedState();

  protected abstract Stream<OplogOperation> streamOplog();

  @Nullable
  protected abstract Class<? extends Exception> getExpectedExceptionClass();

  @Override
  public void execute(Context context) throws Exception {
    MongodServer server = context.getMongodServer();

    try (MongodSchemaExecutor schemaEx = server.openSchemaExecutor()) {
      prepare(schemaEx);
    }
    RETRIER.retry(() -> {
      try (WriteMongodTransaction trans = server.openWriteTransaction()) {
        given(trans);
        trans.commit();
        return null;
      } catch (TimeoutException | UserException ex) {
        throw new RetrierAbortException(ex);
      }
    });

    Exception error = null;
    try {
      context.apply(streamOplog(), getApplierContext());
    } catch (Exception t) {
      error = t;
    }

    try (MongodTransaction trans = server.openReadTransaction()) {
      then(trans, error);
    }
  }

  protected ApplierContext getApplierContext() {
    return new ApplierContext.Builder()
        .setReapplying(true)
        .setUpdatesAsUpserts(true)
        .build();
  }

  @Override
  public Optional<String> getTestName() {
    return Optional.of(this.getClass().getSimpleName());
  }

  @Override
  public boolean shouldIgnore() {
    return false;
  }

  protected void prepare(MongodSchemaExecutor schemaEx) {
    for (DatabaseState db : getInitialState()) {
      String dbName = db.getName();
      for (CollectionState col : db.getCollections()) {
        String colName = col.getName();
        schemaEx.getDocSchemaExecutor().prepareSchema(dbName, colName, col.getDocs());
      }
    }
  }

  protected void given(WriteMongodTransaction trans) throws UserException, RollbackException {
    for (DatabaseState db : getInitialState()) {
      String dbName = db.getName();
      for (CollectionState col : db.getCollections()) {
        String colName = col.getName();
        trans.getDocTransaction().insert(dbName, colName,
            col.getDocs().stream());
      }
    }
  }

  protected void then(MongodTransaction trans, Exception error) throws Exception {
    checkError(error);

    Collection<DatabaseState> expectedState = getExpectedState();

    try (DocTransaction torodTrans = trans.getDocTransaction()) {
      for (DatabaseState db : expectedState) {
        String dbName = db.getName();
        for (CollectionState col : db.getCollections()) {
          String colName = col.getName();

          Map<KvValue<?>, KvDocument> storedDocs = torodTrans
              .findAll(dbName, colName)
              .asDocCursor()
              .transform(toroDoc -> toroDoc.getRoot())
              .getRemaining()
              .stream()
              .collect(Collectors.toMap(
                  doc -> doc.get("_id"),
                  doc -> doc)
              );

          for (KvDocument expectedDoc : col.getDocs()) {
            KvValue<?> id = expectedDoc.get("_id");
            assert id != null : "The test is incorrect, as " + expectedDoc + " does not have _id";

            KvDocument storedDoc = storedDocs.get(id);
            assertTrue("It was expected that " + db.getName() + "." + col.getName() + " contains a "
                + "document with _id " + id, storedDoc != null);
            assertTrue("The document on " + db.getName() + "." + col.getName() + " whose id is "
                + id + " is different than expected. Expected: <" + expectedDoc + "> but was: <"
                + storedDoc + ">", UnorderedDocEquals.equals(expectedDoc, storedDoc));
          }

          assertEquals("Unexpected size on " + dbName + "." + colName,
              col.getDocs().size(),
              storedDocs.size());
        }
      }

      Set<String> foundNs = torodTrans.getDatabases().stream()
          .filter(dbName -> !dbName.equals("torodb"))
          .flatMap(dbName -> torodTrans.getCollectionsInfo(dbName)
              .map(colInfo -> dbName + '.' + colInfo.getName())
          ).collect(Collectors.toSet());
      Set<String> expectedNs = expectedState.stream()
          .flatMap(db -> db.getCollections().stream()
              .map(col -> db.getName() + '.' + col.getName())
          ).collect(Collectors.toSet());

      Truth.assertWithMessage("Unexpected namespaces")
          .that(foundNs)
          .containsExactlyElementsIn(expectedNs);

      Set<String> expectedDbNames = expectedState.stream()
          .map(DatabaseState::getName)
          .collect(Collectors.toSet());
      Set<String> foundDbNames = trans.getDocTransaction()
          .getDatabases()
          .stream()
          .filter(dbName -> !dbName.equals("torodb"))
          .collect(Collectors.toSet());
      Truth.assertWithMessage("Unexpected databases")
          .that(foundDbNames)
          .containsExactlyElementsIn(expectedDbNames);
    }
  }

  private void checkError(Exception error) throws Exception {
    Class<? extends Throwable> expectedThrowableClass = getExpectedExceptionClass();
    if (error == null) { //no error found on the test
      if (expectedThrowableClass == null) { //no error was expected
        return; //everything is fine
      } else {
        Assert.fail("The execution completed successfully, but a "
            + expectedThrowableClass.getSimpleName() + " was expected to be thrown");
      }
    } else {
      if (expectedThrowableClass == null) {
        throw error;
      } else {
        if (expectedThrowableClass.isAssignableFrom(error.getClass())) {
          throw new AssertionError("It was expected that the execution throws a "
              + expectedThrowableClass.getName() + " but " + error + " was found", error);
        }
      }
    }
  }

  public static class DatabaseState {

    private final String name;
    private final Collection<CollectionState> collections;

    public DatabaseState(String name, Stream<CollectionState> cols) {
      this.name = name;
      this.collections = cols.collect(Collectors.toList());
    }

    public String getName() {
      return name;
    }

    public Collection<CollectionState> getCollections() {
      return collections;
    }
  }

  public static class CollectionState {

    private final String name;
    private final Set<KvDocument> docs;

    public CollectionState(String name, Stream<KvDocument> docs) {
      this.name = name;
      this.docs = docs.collect(Collectors.toSet());
    }

    public String getName() {
      return name;
    }

    public Set<KvDocument> getDocs() {
      return docs;
    }
  }

}
