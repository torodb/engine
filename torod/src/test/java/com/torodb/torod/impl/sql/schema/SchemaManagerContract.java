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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.torodb.core.TableRefFactory;
import com.torodb.core.backend.DdlOperationExecutor;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaScalar;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.WrapperMutableMetaCollection;
import com.torodb.kvdocument.conversion.json.JacksonJsonParser;
import com.torodb.kvdocument.conversion.json.JsonParser;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.torod.exception.AlreadyExistentCollectionException;
import com.torodb.torod.exception.UnexistentCollectionException;
import com.torodb.torod.exception.UnexistentDatabaseException;
import org.hamcrest.CoreMatchers;
import org.hamcrest.CustomMatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ContainerExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ObjectArrayArguments;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;

@RunWith(JUnitPlatform.class)
public abstract class SchemaManagerContract {

  private SchemaManager sm;
  private DdlOperationExecutor ddlOpsEx;
  //this should only be used on the setup methods
  private DdlOperationExecutor refreshDdlOps;
  private final TableRefFactory tableRefFactory = new TableRefFactoryImpl();
  private static final JsonParser PARSER = new JacksonJsonParser();

  @BeforeEach
  void setUp() {
    sm = createManager();
    sm.startAsync();
    sm.awaitRunning();
    refreshDdlOps = Mockito.mock(DdlOperationExecutor.class);
    ddlOpsEx = Mockito.mock(DdlOperationExecutor.class);

    /*
     * For historical reasons, DdlOperationExecutor#rename is responsible to copy childrean meta
     * elements from the original collection to the new one. This should be removed on future
     * versions, but meanwhile the mock has to simmulate that to be able to test the correct
     * behaviour of a SchemaManager that is the single object that modifies the schema
     * metainformation
     */
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        MetaCollection fromCol = (MetaCollection) invocation.getArgument(1);
        MutableMetaCollection toCol = (MutableMetaCollection) invocation.getArgument(3);

        fromCol.streamContainedMetaDocParts().forEach(dp -> toCol.addMetaDocPart(
            dp.getTableRef(),
            dp.getIdentifier())
        );

        return null;
      }
    }).when(ddlOpsEx).renameCollection(any(), any(), any(), any());
  }

  @AfterEach
  void tearDown() {
    if (sm != null && sm.isRunning()) {
      sm.stopAsync();
    }
  }

  protected TableRefFactory getTableRefFactory() {
    return tableRefFactory;
  }

  protected abstract SchemaManager createManager();
  
  protected abstract DocSchemaAnalyzer getDocSchemaAnalyzer();

  @Nested
  class PrepareSchema {

    private ImmutableMetaSnapshot initialSnapshot = new ImmutableMetaSnapshot.Builder()
        .put(new ImmutableMetaDatabase("oldDbName", "oldDbId", Collections.singleton(
            new ImmutableMetaCollection.Builder("oldColName", "oldColId")
                .put(new ImmutableMetaDocPart.Builder(tableRefFactory.createRoot(), "root")
                    .put(new ImmutableMetaField("parentField", "parentField", FieldType.INTEGER))
                    .put(new ImmutableMetaField("path1", "path1_c", FieldType.CHILD))
                )
                .put(new ImmutableMetaDocPart.Builder(
                    tableRefFactory.createChild(tableRefFactory.createRoot(), "path1"), "path1")
                    .put(new ImmutableMetaField("childField", "childField", FieldType.STRING))
                    .put(new ImmutableMetaScalar("childScalar", FieldType.INTEGER))
                )
                .build()
        )))
        .build();
      

    @BeforeEach
    void refresh() {
      doReturn(initialSnapshot)
          .when(refreshDdlOps)
          .readMetadata();
      sm.refreshMetadata(refreshDdlOps).join();
    }

    private void okCase(String dbName, String colName, Collection<KvDocument> docs) {
      sm.prepareSchema(ddlOpsEx, dbName, colName, docs).join();

      //Then
      ImmutableMetaSnapshot finalSnashot = sm.getMetaSnapshot().join();
      ImmutableMetaDatabase db = finalSnashot.getMetaDatabaseByName(dbName);
      assertThat(db, notNullValue());
      ImmutableMetaCollection col = db.getMetaCollectionByName(colName);
      assertThat(col, notNullValue());
      MutableMetaCollection mutableCol = new WrapperMutableMetaCollection(col);
      getDocSchemaAnalyzer().analyze(
          db,
          mutableCol,
          docs.stream()
      );

      Assertions.assertFalse(mutableCol.hasChanges(), "After calling PrepareSchema, there are some"
          + "changes that must be done to insert the documents");
    }

    @ParameterizedTest
    @ArgumentsSource(StreamDocuments.class)
    void whenDbDoesNotExist(Collection<KvDocument> docs) throws UserException {
      String dbName = "newDbName";
      String colName = "dontcare";

      okCase(dbName, colName, docs);

      //Then
      MetaSnapshot finalSnapshot = sm.getMetaSnapshot().join();
      MetaDatabase db = finalSnapshot.getMetaDatabaseByName(dbName);
      verify(ddlOpsEx).addDatabase(argThat(matchDb(db)));
      MetaCollection col = db.getMetaCollectionByName(colName);
      verify(ddlOpsEx).addCollection(
          argThat(matchDb(db)),
          argThat(matchCol(col))
      );
    }

    @ParameterizedTest
    @ArgumentsSource(StreamDocuments.class)
    void whenColDoesNotExist(Collection<KvDocument> docs) {
      String dbName = "oldDbName";
      String colName = "newColName";

      okCase(dbName, colName, docs);

      //Then
      MetaSnapshot finalSnapshot = sm.getMetaSnapshot().join();
      MetaDatabase db = finalSnapshot.getMetaDatabaseByName(dbName);
      verify(ddlOpsEx, times(0)).addDatabase(argThat(matchDb(db)));
      MetaCollection col = db.getMetaCollectionByName(colName);
      verify(ddlOpsEx).addCollection(
          argThat(matchDb(db)),
          argThat(matchCol(col))
      );

    }

    @ParameterizedTest
    @ArgumentsSource(StreamDocuments.class)
    void whenDontFit(Collection<KvDocument> docs) {
      String dbName = "oldDbName";
      String colName = "oldColName";

      okCase(dbName, colName, docs);

      //Then
      MetaSnapshot finalSnapshot = sm.getMetaSnapshot().join();
      MetaDatabase db = finalSnapshot.getMetaDatabaseByName(dbName);
      verify(ddlOpsEx, times(0)).addDatabase(argThat(matchDb(db)));
      MetaCollection col = db.getMetaCollectionByName(colName);
      verify(ddlOpsEx, times(0)).addCollection(
          argThat(matchDb(db)),
          argThat(matchCol(col))
      );
    }

    @ParameterizedTest
    @ArgumentsSource(StreamFitDocuments.class)
    void whenFit(Collection<KvDocument> docs) {
      String dbName = "oldDbName";
      String colName = "oldColName";

      okCase(dbName, colName, docs);

      //Then
      MetaSnapshot finalSnapshot = sm.getMetaSnapshot().join();
      MetaDatabase db = finalSnapshot.getMetaDatabaseByName(dbName);
      verify(ddlOpsEx, times(0)).addDatabase(argThat(matchDb(db)));
      MetaCollection col = db.getMetaCollectionByName(colName);
      verify(ddlOpsEx, times(0)).addCollection(
          argThat(matchDb(db)),
          argThat(matchCol(col))
      );
    }
  }

  @Nested
  class CreateDatabase {

    @BeforeEach
    void refresh() {
      ImmutableMetaSnapshot snapshot = new ImmutableMetaSnapshot.Builder()
          .put(new ImmutableMetaDatabase("oldDbName", "oldDbId", Collections.emptyList()))
          .build();
      doReturn(snapshot)
          .when(refreshDdlOps)
          .readMetadata();
      sm.refreshMetadata(refreshDdlOps).join();
    }

    @Test
    void whenDbDoesNotExist() {
      //Given
      String dbName = "newDbName";

      //When
      sm.createDatabase(ddlOpsEx, dbName)
          .join();

      //Then
      ImmutableMetaDatabase foundDb = sm.getMetaSnapshot().join().getMetaDatabaseByName(dbName);
      assertThat(foundDb, notNullValue());
      assertThat(foundDb.getName(), is(equalTo(dbName)));

      //ddlOpsEx must recive a request to create a db with the same name and id than the returned db
      verify(ddlOpsEx).addDatabase(argThat(matchDb(foundDb)));
    }

    @Test
    void whenDbExist() {
      //Given
      String dbName = "oldDbName";

      //When
      sm.createDatabase(ddlOpsEx, dbName)
          .join();

      //Then
      ImmutableMetaDatabase foundDb = sm.getMetaSnapshot().join().getMetaDatabaseByName(dbName);
      assertThat(foundDb, notNullValue());
      assertThat(foundDb.getName(), is(equalTo(dbName)));

      verify(ddlOpsEx, times(0)).addDatabase(foundDb);
    }
  }

  @Nested
  class DropDatabase {

    private final ImmutableMetaSnapshot initialSnapshot = new ImmutableMetaSnapshot.Builder()
        .put(new ImmutableMetaDatabase("oldDbName", "oldDbId", Collections.emptyList()))
        .build();

    @BeforeEach
    void refresh() {
      doReturn(initialSnapshot)
          .when(refreshDdlOps)
          .readMetadata();
      sm.refreshMetadata(refreshDdlOps).join();
    }

    @Test
    void whenDbDoesNotExist() {
      //Given
      String dbName = "newDbName";

      //When
      sm.dropDatabase(ddlOpsEx, dbName)
          .join();

      //Then
      ImmutableMetaDatabase foundDb = sm.getMetaSnapshot().join().getMetaDatabaseByName(dbName);
      assertThat(foundDb, nullValue());

      //ddlOpsEx must not recive a request to drop the db
      verify(ddlOpsEx, times(0)).dropDatabase(foundDb);
    }

    @Test
    void whenDbExist() {
      //Given
      String dbName = "oldDbName";
      MetaDatabase initialDb = initialSnapshot.getMetaDatabaseByName(dbName);
      assert initialDb != null;

      //When
      sm.dropDatabase(ddlOpsEx, dbName)
          .join();

      //Then
      ImmutableMetaDatabase foundDb = sm.getMetaSnapshot().join().getMetaDatabaseByName(dbName);
      assertThat(foundDb, nullValue());

      //ddlOpsEx must recive a request to drop a db with the same name and id
      verify(ddlOpsEx).dropDatabase(argThat(matchDb(initialDb)));
    }

  }

  @Nested
  class CreateCollection {

    private final ImmutableMetaSnapshot initialSnapshot = new ImmutableMetaSnapshot.Builder()
        .put(new ImmutableMetaDatabase("oldDbName", "oldDbId", Collections.singleton(
            new ImmutableMetaCollection.Builder("oldColName", "oldColId").build()
        )))
        .build();

    @BeforeEach
    void refresh() {
      doReturn(initialSnapshot)
          .when(refreshDdlOps)
          .readMetadata();
      sm.refreshMetadata(refreshDdlOps).join();
    }

    @Test
    void whenDbDoesNotExist() {
      //Given
      String dbName = "newDbName";
      assert initialSnapshot.getMetaDatabaseByName(dbName) == null;
      String colName = "newColName";

      //When
      CompletionException ex = Assertions.assertThrows(CompletionException.class, () ->
          sm.createCollection(ddlOpsEx, dbName, colName).join()
      );

      //Then
      assertThat(ex.getCause(), is(CoreMatchers.instanceOf(UnexistentDatabaseException.class)));
      verify(ddlOpsEx, times(0)).addCollection(any(), any());
    }

    @Test
    void whenColDoesNotExist() {
      //Given
      String dbName = "oldDbName";
      MetaDatabase initialDb = initialSnapshot.getMetaDatabaseByName(dbName);
      assert initialDb != null;
      String colName = "newColName";
      assert initialDb.getMetaCollectionByName(colName) == null;

      //When
      sm.createCollection(ddlOpsEx, dbName, colName).join();

      //Then
      ImmutableMetaDatabase foundDb = sm.getMetaSnapshot().join().getMetaDatabaseByName(dbName);
      assertThat(foundDb, notNullValue());
      ImmutableMetaCollection foundCol = foundDb.getMetaCollectionByName(colName);
      assertThat(foundCol, notNullValue());
      assertThat(foundCol.getName(), is(equalTo(colName)));

      //ddlOpsEx must recive a request to add that collection
      verify(ddlOpsEx).addCollection(argThat(matchDb(initialDb)), argThat(matchCol(foundCol)));
    }

    @Test
    void whenColExist() {
      //Given
      String dbName = "oldDbName";
      assert initialSnapshot.getMetaDatabaseByName(dbName) != null;
      String colName = "oldColName";
      assert initialSnapshot.getMetaDatabaseByName(dbName).getMetaCollectionByName(colName) != null;

      //When
      sm.createCollection(ddlOpsEx, dbName, colName).join();

      //Then
      verify(ddlOpsEx, times(0)).addCollection(any(), any());
    }

  }

  @Nested
  class DropCollection {

    private final ImmutableMetaSnapshot initialSnapshot = new ImmutableMetaSnapshot.Builder()
        .put(new ImmutableMetaDatabase("oldDbName", "oldDbId", Collections.singleton(
            new ImmutableMetaCollection.Builder("oldColName", "oldColId").build()
        )))
        .build();

    @BeforeEach
    void refresh() {
      doReturn(initialSnapshot)
          .when(refreshDdlOps)
          .readMetadata();
      sm.refreshMetadata(refreshDdlOps).join();
    }

    @Test
    void whenDbDoesNotExist() {
      //Given
      String dbName = "newDbName";
      assert initialSnapshot.getMetaDatabaseByName(dbName) == null;
      String colName = "newColName";

      //When
      CompletionException ex = Assertions.assertThrows(CompletionException.class, () ->
          sm.dropCollection(ddlOpsEx, dbName, colName).join()
      );

      //Then
      assertThat(ex.getCause(), is(CoreMatchers.instanceOf(UnexistentDatabaseException.class)));
      verify(ddlOpsEx, times(0)).dropCollection(any(), any());
    }

    @Test
    void whenColDoesNotExist() {
      //Given
      String dbName = "oldDbName";
      MetaDatabase initialDb = initialSnapshot.getMetaDatabaseByName(dbName);
      assert initialDb != null;
      String colName = "newColName";
      assert initialDb.getMetaCollectionByName(colName) == null;

      //When
      sm.dropCollection(ddlOpsEx, dbName, colName).join();

      //Then
      ImmutableMetaDatabase foundDb = sm.getMetaSnapshot().join().getMetaDatabaseByName(dbName);
      assertThat(foundDb, notNullValue());
      ImmutableMetaCollection foundCol = foundDb.getMetaCollectionByName(colName);
      assertThat(foundCol, nullValue());

      verify(ddlOpsEx, times(0)).dropCollection(any(), any());
    }

    @Test
    void whenColExist() {
      //Given
      String dbName = "oldDbName";
      MetaDatabase initialDb = initialSnapshot.getMetaDatabaseByName(dbName);
      assert initialDb != null;
      String colName = "oldColName";
      MetaCollection initialCol = initialSnapshot.getMetaDatabaseByName(dbName)
          .getMetaCollectionByName(colName);
      assert initialCol != null;

      //When
      sm.dropCollection(ddlOpsEx, dbName, colName).join();

      //Then
      verify(ddlOpsEx, times(0)).addCollection(any(), any());
      ImmutableMetaDatabase foundDb = sm.getMetaSnapshot().join().getMetaDatabaseByName(dbName);
      assertThat(foundDb, notNullValue());
      ImmutableMetaCollection foundCol = foundDb.getMetaCollectionByName(colName);
      assertThat(foundCol, nullValue());

      //ddlOpsEx must recive a request to add that collection
      verify(ddlOpsEx, times(1)).dropCollection(
          argThat(matchDb(initialDb)),
          argThat(matchCol(initialCol))
      );
    }

  }

  @Nested
  class RenameCollection {

    private final ImmutableMetaSnapshot initialSnapshot = new ImmutableMetaSnapshot.Builder()
        .put(new ImmutableMetaDatabase("oldDbName", "oldDbId", Collections.singleton(
            new ImmutableMetaCollection.Builder("oldColName", "oldColId")
                .put(new ImmutableMetaDocPart(tableRefFactory.createRoot(), "root"))
                .build()
        )))
        .put(new ImmutableMetaDatabase("oldDbName2", "oldDbId2", Collections.singleton(
            new ImmutableMetaCollection.Builder("oldColName2", "oldColId2").build()
        )))
        .build();

    @BeforeEach
    void refresh() {
      doReturn(initialSnapshot)
          .when(refreshDdlOps)
          .readMetadata();
      sm.refreshMetadata(refreshDdlOps).join();
    }

    private MetaSnapshot okCase(String fromDbName, String fromColName,
        String toDbName, String toColName) {

      MetaCollection fromCol = initialSnapshot.getMetaDatabaseByName(fromDbName)
          .getMetaCollectionByName(fromColName);
      assert fromCol != null;

      //When
      sm.renameCollection(ddlOpsEx, fromDbName, fromColName, toDbName, toColName);

      //Then
      MetaSnapshot finalSnapshot = sm.getMetaSnapshot().join();
      MetaDatabase fromDb = finalSnapshot.getMetaDatabaseByName(fromDbName);
      CustomMatcher<MetaDatabase> dontContainFromCol = new CustomMatcher<MetaDatabase>(
          "Should not contain 'fromCol'") {
        @Override
        public boolean matches(Object item) {
          return (item instanceof MetaDatabase)
              && ((MetaDatabase) item).getMetaCollectionByName(fromColName) != null;
        }
      };
      assertThat(fromDb, anyOf(notNullValue(), dontContainFromCol));

      MetaDatabase toDb = finalSnapshot.getMetaDatabaseByName(toDbName);
      assertThat(toDb, notNullValue());
      
      MetaCollection toCol = toDb.getMetaCollectionByName(toColName);
      assertNotNull(toCol, "'toDb' does not contain 'toCol'");

      fromCol.streamContainedMetaDocParts().forEach(originalDocPart ->
          assertNotNull(toCol.getMetaDocPartByTableRef(originalDocPart.getTableRef()),
              "Original doc part " + originalDocPart + " was not moved to 'toCol'")
      );

      verify(ddlOpsEx).renameCollection(
          argThat(matchDb(fromDb)),
          argThat(matchCol(fromCol)),
          argThat(matchDb(toDb, MutableMetaDatabase.class)), 
          argThat(matchCol(toCol, MutableMetaCollection.class))
      );

      return finalSnapshot;
    }

    @Test
    void whenFromDbDoesNotExist() {
      //Given
      String fromDbName = "newDbName";
      assert initialSnapshot.getMetaDatabaseByName(fromDbName) == null;
      String fromColName = "dontcare1";
      String toDbName = "dontcare2";
      String toColName = "dontcare3";

      //When
      CompletionException ex = Assertions.assertThrows(CompletionException.class, () ->
          sm.renameCollection(ddlOpsEx, fromDbName, fromColName, toDbName, toColName).join()
      );

      //Then
      assertThat(ex.getCause(), is(CoreMatchers.instanceOf(UnexistentDatabaseException.class)));
      verify(ddlOpsEx, times(0)).renameCollection(any(), any(), any(), any());
    }

    @Nested
    class WhenFromDbExists {

      private final String fromDbName = "oldDbName";

      @Test
      void whenFromColDoesNotExist() {

        //Given
        String fromColName = "newColName";
        String toDbName = "newDbName2";
        String toColName = "newColName2";

        //When
        CompletionException ex = Assertions.assertThrows(CompletionException.class, () ->
            sm.renameCollection(ddlOpsEx, fromDbName, fromColName, toDbName, toColName).join()
        );

        //Then
        assertThat(ex.getCause(), is(CoreMatchers.instanceOf(UnexistentCollectionException.class)));

        verify(ddlOpsEx, times(0)).renameCollection(any(), any(), any(), any());

      }

      @Nested
      class ToColExists {

        private final String fromColName = "oldColName";

        private void alreadyExistentCase(String toDbName, String toColName) {
          //When
          CompletionException ex = Assertions.assertThrows(CompletionException.class, () ->
              sm.renameCollection(ddlOpsEx, fromDbName, fromColName, toDbName, toColName).join()
          );

          //Then
          assertThat(ex.getCause(), is(CoreMatchers.instanceOf(
              AlreadyExistentCollectionException.class))
          );

          verify(ddlOpsEx, times(0)).renameCollection(any(), any(), any(), any());
        }

        @Test
        void andToDbDoesNotExist() {
          String toDbName = "newColName2";
          String toColName = "dontcare";

          MetaSnapshot finalSnapshot = okCase(fromDbName, fromColName, toDbName, toColName);

          verify(ddlOpsEx).addDatabase(
              argThat(matchDb(finalSnapshot.getMetaDatabaseByName(toDbName)))
          );
          MetaDatabase toDb = finalSnapshot.getMetaDatabaseByName(toDbName);
          verify(ddlOpsEx).addCollection(
              argThat(matchDb(toDb)),
              argThat(matchCol(toDb.getMetaCollectionByName(toColName)))
          );
        }

        @Nested
        class ToDbIsTheSame {
          private final String toDbName = fromDbName;

          @Test
          void andToColIsTheSame() {
            String toColName = fromColName;
            alreadyExistentCase(toDbName, toColName);
            verifyNoMoreInteractions(ddlOpsEx);
          }

          @Test
          void andToColDoesNotExist() {
            String toColName = "newColName2";

            MetaSnapshot finalSnapshot = okCase(fromDbName, fromColName, toDbName, toColName);

            MetaDatabase toDb = finalSnapshot.getMetaDatabaseByName(toDbName);
            verify(ddlOpsEx).addCollection(
                argThat(matchDb(toDb)),
                argThat(matchCol(toDb.getMetaCollectionByName(toColName)))
            );
          }

          void andToColExists() {
            String toColName = "oldColName2";
            alreadyExistentCase(toDbName, toColName);
          }

        }

        @Nested
        class ToDbExists {
          private final String toDbName = "oldDbName2";
          
          @Test
          void butToColDoesNotExist() {
            String toColName = "newColName2";

            MetaSnapshot finalSnapshot = okCase(fromDbName, fromColName, toDbName, toColName);

            MetaDatabase toDb = finalSnapshot.getMetaDatabaseByName(toDbName);
            verify(ddlOpsEx, times(0)).addDatabase(argThat(matchDb(toDb)));
            verify(ddlOpsEx).addCollection(
                argThat(matchDb(toDb)),
                argThat(matchCol(toDb.getMetaCollectionByName(toColName)))
            );

          }
          
          @Test
          void andToColExists() {
            String toColName = "oldColName2";
            alreadyExistentCase(toDbName, toColName);
          }
        }

      }
    }

  }

  @Nested
  class DisableDataImportMode {

    private final ImmutableMetaSnapshot initialSnapshot = new ImmutableMetaSnapshot.Builder()
        .put(new ImmutableMetaDatabase("oldDbName", "oldDbId", Collections.singleton(
            new ImmutableMetaCollection.Builder("oldColName", "oldColId").build()
        )))
        .build();

    @BeforeEach
    void refresh() {
      doReturn(initialSnapshot)
          .when(refreshDdlOps)
          .readMetadata();
      sm.refreshMetadata(refreshDdlOps).join();
    }

    @Test
    void throwIfDbDoesNotExist() {
      //Given
      String dbName = "newDbName";

      //When
      CompletionException ex = Assertions.assertThrows(CompletionException.class, () ->
          sm.disableDataImportMode(ddlOpsEx, dbName).join()
      );

      //Then
      assertThat(ex.getCause(), is(CoreMatchers.instanceOf(UnexistentDatabaseException.class)));
      verify(ddlOpsEx, times(0)).disableDataImportMode(any());
    }

    @Test
    void whenDbExists() {
      //Given
      String dbName = "oldDbName";

      //When
      sm.disableDataImportMode(ddlOpsEx, dbName).join();

      //Then
      verify(ddlOpsEx).disableDataImportMode(any());

    }
  }

  @Nested
  class EnableDataImportMode {

    private final ImmutableMetaSnapshot initialSnapshot = new ImmutableMetaSnapshot.Builder()
        .put(new ImmutableMetaDatabase("oldDbName", "oldDbId", Collections.emptyList()))
        .build();

    @BeforeEach
    void refresh() {
      doReturn(initialSnapshot)
          .when(refreshDdlOps)
          .readMetadata();
      sm.refreshMetadata(refreshDdlOps).join();
    }

    @Test
    void throwIfDbDoesNotExist() {
      //Given
      String dbName = "newDbName";

      //When
      CompletionException ex = Assertions.assertThrows(CompletionException.class, () ->
          sm.enableDataImportMode(ddlOpsEx, dbName).join()
      );

      //Then
      assertThat(ex.getCause(), is(CoreMatchers.instanceOf(UnexistentDatabaseException.class)));
      verify(ddlOpsEx, times(0)).enableDataImportMode(any());
    }

    @Test
    void whenDbExists() {
      //Given
      String dbName = "oldDbName";

      //When
      sm.enableDataImportMode(ddlOpsEx, dbName).join();

      //Then
      verify(ddlOpsEx).enableDataImportMode(any());

    }
  }

  /**
   * Returns a db {@link ArgumentMatcher} that is evaluated to true when the name and the id of the
   * found db is the same as the expected.
   */
  private static ArgumentMatcher<? extends MetaDatabase> matchDb(MetaDatabase expectedDb) {
    return (foundDb) -> expectedDb.getName().equals(foundDb.getName())
        && expectedDb.getIdentifier().equals(foundDb.getIdentifier());
  }

  private static <T extends MetaDatabase> ArgumentMatcher<T> matchDb(MetaDatabase expectedDb,
      Class<T> dbClass) {
    return (foundDb) -> expectedDb.getName().equals(foundDb.getName())
        && expectedDb.getIdentifier().equals(foundDb.getIdentifier());
  }

  /**
   * Returns a col {@link ArgumentMatcher} that is evaluated to true when the name and the id of the
   * found col is the same as the expected.
   */
  private static ArgumentMatcher<? extends MetaCollection> matchCol(MetaCollection expectedCol) {
    return (foundCol) -> expectedCol.getName().equals(foundCol.getName())
        && expectedCol.getIdentifier().equals(foundCol.getIdentifier());
  }
  private static <T extends MetaCollection> ArgumentMatcher<T> matchCol(MetaCollection expectedCol,
      Class<T> colClass) {
    return (foundCol) -> expectedCol.getName().equals(foundCol.getName())
        && expectedCol.getIdentifier().equals(foundCol.getIdentifier());
  }

  static class StreamDocuments implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> arguments(ContainerExtensionContext context) throws Exception {

      Stream<Arguments> unfitDocs = Stream.of(
          "{\"newField\": 1}",
          "{\"newField\": \"a text\"}",
          "{\"path1\": {\"newField\": 3.1416}}",
          "{\"path1\": [1,2,3,\"text\"]}"
      ).map(PARSER::createFromJson)
          .map(Collections::singleton)
          .map(col -> ObjectArrayArguments.create(col));

      return Stream.concat(unfitDocs, new StreamFitDocuments().arguments(context));
    }

  }

  static class StreamFitDocuments implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> arguments(ContainerExtensionContext context) throws Exception {

      return Stream.of(
          "{}",
          "{\"parentField\": 2}",
          "{\"path1\": {}}",
          "{\"path1\": {\"childField\": \"text\"}}",
          "{\"path1\": [1,2,3]}"
      ).map(PARSER::createFromJson)
          .map(Collections::singleton)
          .map(col -> ObjectArrayArguments.create(col));
    }

  }

}