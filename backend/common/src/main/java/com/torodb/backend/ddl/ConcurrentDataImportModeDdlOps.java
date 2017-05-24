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

package com.torodb.backend.ddl;

import com.google.common.base.Throwables;
import com.torodb.backend.BackendLoggerFactory;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.StructureInterface;
import com.torodb.common.util.CompletionExceptions;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.concurrent.StreamExecutor;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.exceptions.ToroRuntimeException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.lambda.tuple.Tuple3;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.inject.Inject;

/**
 *
 */
public class ConcurrentDataImportModeDdlOps implements DataImportModeDdlOps {

  private static final Logger LOGGER = BackendLoggerFactory.get(
      ConcurrentDataImportModeDdlOps.class);

  private final SqlInterface sqlInterface;
  private final StreamExecutor streamExecutor;
  private final Retrier retrier;

  @Inject
  public ConcurrentDataImportModeDdlOps(
      SqlInterface sqlInterface,
      ConcurrentToolsFactory concurrentToolsFactory,
      Retrier retrier) {
    this.sqlInterface = sqlInterface;
    this.streamExecutor = concurrentToolsFactory.createStreamExecutor(
        LOGGER, "backend-inner-jobs", true);
    this.retrier = retrier;
    streamExecutor.startAsync();
    streamExecutor.awaitRunning();
  }

  @Override
  public void enableDataImportMode(MetaDatabase db) throws RollbackException {
    if (!sqlInterface.getDbBackend().isOnDataInsertMode(db)) {
      sqlInterface.getDbBackend().enableDataInsertMode(db);
    }
  }

  @Override
  public void disableDataImportMode(DSLContext dsl, MetaDatabase db) throws RollbackException {
    if (!sqlInterface.getDbBackend().isOnDataInsertMode(db)) {
      LOGGER.debug("Ignoring attempt to disable import mode on {} as it is not on that mode",
          db.getIdentifier());
      return ;
    }
    sqlInterface.getDbBackend().disableDataInsertMode(db);

    //create internal indexes
    Stream<Consumer<DSLContext>> createInternalIndexesJobs = db.streamMetaCollections().flatMap(
        col -> col.streamContainedMetaDocParts().flatMap(
            docPart -> enableInternalIndexJobs(db, col, docPart)
        )
    );

    //create indexes
    Stream<Consumer<DSLContext>> createIndexesJobs = db.streamMetaCollections().flatMap(
        col -> enableIndexJobs(db, col)
    );

    //backend specific jobs
    Stream<Consumer<DSLContext>> backendSpecificJobs = sqlInterface
        .getStructureInterface()
        .streamDataInsertFinishTasks(db).map(job -> {
          return (Consumer<DSLContext>) _dsl -> {
            String index = job.apply(_dsl);
            LOGGER.info("Task {} completed", index);
          };
        });
    Stream<Consumer<DSLContext>> jobs = Stream
        .concat(createInternalIndexesJobs, createIndexesJobs);
    jobs = Stream.concat(jobs, backendSpecificJobs);
    Stream<Runnable> runnables = jobs.map(jobConsumer -> dslConsumerToRunnable(dsl, jobConsumer));

    try {
      streamExecutor.executeRunnables(runnables).join();
    } catch (CompletionException ex) {
      Throwable cause = CompletionExceptions.getFirstNonCompletionException(ex);
      Throwables.throwIfUnchecked(cause);
      throw ex;
    }
  }

  private Stream<Consumer<DSLContext>> enableInternalIndexJobs(
      MetaDatabase db, MetaCollection col, MetaDocPart docPart) {
    StructureInterface structureInterface = sqlInterface.getStructureInterface();

    Stream<Function<DSLContext, String>> consumerStream;

    if (docPart.getTableRef().isRoot()) {
      consumerStream = structureInterface.streamRootDocPartTableIndexesCreation(
          db.getIdentifier(),
          docPart.getIdentifier(),
          docPart.getTableRef()
      );
    } else {
      MetaDocPart parentDocPart = col.getMetaDocPartByTableRef(
          docPart.getTableRef().getParent().get()
      );
      assert parentDocPart != null;
      consumerStream = structureInterface.streamDocPartTableIndexesCreation(
          db.getIdentifier(),
          docPart.getIdentifier(),
          docPart.getTableRef(),
          parentDocPart.getIdentifier()
      );
    }

    return consumerStream.map(job -> {
      return (Consumer<DSLContext>) _dsl -> {
        String index = job.apply(_dsl);
        LOGGER.info("Created internal index {} for table {}", index, docPart.getIdentifier());
      };
    });
  }

  private Stream<Consumer<DSLContext>> enableIndexJobs(
      MetaDatabase db, MetaCollection col) {
    List<Consumer<DSLContext>> consumerList = new ArrayList<>();

    Iterator<? extends MetaDocPart> docPartIterator = col.streamContainedMetaDocParts().iterator();
    while (docPartIterator.hasNext()) {
      MetaDocPart docPart = docPartIterator.next();

      Iterator<? extends MetaIdentifiedDocPartIndex> docPartIndexIterator = docPart.streamIndexes()
          .iterator();
      while (docPartIndexIterator.hasNext()) {
        MetaIdentifiedDocPartIndex docPartIndex = docPartIndexIterator.next();

        consumerList.add(createIndexJob(db, docPart, docPartIndex));
      }
    }

    return consumerList.stream();
  }

  private Consumer<DSLContext> createIndexJob(MetaDatabase db, MetaDocPart docPart,
      MetaIdentifiedDocPartIndex docPartIndex) {
    return _dsl -> {
      List<Tuple3<String, Boolean, FieldType>> columnList = new ArrayList<>(docPartIndex.size());
      for (Iterator<? extends MetaDocPartIndexColumn> indexColumnIterator = docPartIndex
          .iteratorColumns(); indexColumnIterator.hasNext();) {
        MetaDocPartIndexColumn indexColumn = indexColumnIterator.next();
        columnList.add(new Tuple3<>(indexColumn.getIdentifier(), indexColumn.getOrdering()
            .isAscending(), 
            docPart.getMetaFieldByIdentifier(indexColumn.getIdentifier()).getType()));
      }

      try {
        sqlInterface.getStructureInterface().createIndex(
            _dsl, docPartIndex.getIdentifier(), db.getIdentifier(), docPart.getIdentifier(),
            columnList,
            docPartIndex.isUnique());
      } catch (UserException userException) {
        throw new SystemException(userException);
      }
      LOGGER.info("Created index {} for table {}", docPartIndex.getIdentifier(), docPart
          .getIdentifier());
    };
  }

  private Runnable dslConsumerToRunnable(DSLContext dsl, Consumer<DSLContext> consumer) {
    return () -> {
      try {
        retrier.retry(() -> {
          try (Connection connection = sqlInterface
              .getDbBackend()
              .createWriteConnection()) {
            DSLContext threadDsl = sqlInterface.getDslContextFactory().createDslContext(connection);

            consumer.accept(threadDsl);
            connection.commit();
            return null;
          } catch (SQLException ex) {
            throw sqlInterface
                .getErrorHandler()
                .handleException(ErrorHandler.Context.CREATE_INDEX, ex);
          }
        }, Retrier.Hint.CRITICAL);
      } catch (RetrierGiveUpException ex) {
        throw new ToroRuntimeException(ex);
      }
    };
  }

  @Override
  public void close() throws Exception {
    streamExecutor.stopAsync();
  }

}