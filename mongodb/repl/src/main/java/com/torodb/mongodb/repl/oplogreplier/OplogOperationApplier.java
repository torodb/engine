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

package com.torodb.mongodb.repl.oplogreplier;

import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.transaction.RollbackException;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.torodb.mongodb.core.MongodSchemaExecutor;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.repl.commands.ReplCommandExecutor;
import com.torodb.mongodb.repl.commands.ReplCommandLibrary;
import com.torodb.mongodb.repl.oplogreplier.analyzed.AnalyzedOp;
import com.torodb.mongodb.repl.oplogreplier.analyzed.AnalyzedOpReducer;
import com.torodb.mongodb.repl.oplogreplier.batch.NamespaceJob;
import com.torodb.mongodb.repl.oplogreplier.batch.NamespaceJobExecutionException;
import com.torodb.mongodb.repl.oplogreplier.batch.NamespaceJobExecutor;
import com.torodb.mongodb.utils.NamespaceUtil;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.bson.BsonDocument;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.CommandLibrary.LibraryEntry;
import com.torodb.mongowp.commands.Request;
import com.torodb.mongowp.commands.oplog.CollectionOplogOperation;
import com.torodb.mongowp.commands.oplog.DbCmdOplogOperation;
import com.torodb.mongowp.commands.oplog.DbOplogOperation;
import com.torodb.mongowp.commands.oplog.DeleteOplogOperation;
import com.torodb.mongowp.commands.oplog.InsertOplogOperation;
import com.torodb.mongowp.commands.oplog.NoopOplogOperation;
import com.torodb.mongowp.commands.oplog.OplogOperation;
import com.torodb.mongowp.commands.oplog.OplogOperationVisitor;
import com.torodb.mongowp.commands.oplog.UpdateOplogOperation;
import com.torodb.mongowp.exceptions.CommandNotFoundException;
import com.torodb.mongowp.exceptions.MongoException;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

@ThreadSafe
public class OplogOperationApplier {

  private final Logger logger;
  private final Visitor visitor = new Visitor();
  private final ReplCommandLibrary library;
  private final ReplCommandExecutor executor;
  private final NamespaceJobExecutor namespaceJobExecutor;
  private final AnalyzedOpReducer analyzedOpReducer;

  @Inject
  public OplogOperationApplier(ReplCommandLibrary library, ReplCommandExecutor executor,
      LoggerFactory loggerFactory, NamespaceJobExecutor namespaceJobExecutor,
      AnalyzedOpReducer analyzedOpReducer) {
    this.logger = loggerFactory.apply(this.getClass());
    this.library = library;
    this.executor = executor;
    this.namespaceJobExecutor = namespaceJobExecutor;
    this.analyzedOpReducer = analyzedOpReducer;
  }

  /**
   * Applies the given {@link OplogOperation} on the database.
   *
   * <p>This method <b>DO NOT</b> modify the {@link OplogManager} state.
   */
  @SuppressWarnings("unchecked")
  public void apply(OplogOperation op, MongodServer server, ApplierContext applierContext)
      throws OplogApplyingException {
    op.accept(visitor, null).apply(
        server,
        applierContext
    );
  }

  private <A, R> Status<R> executeReplCommand(String db, Command<? super A, ? super R> command,
      A arg, MongodServer server) throws TimeoutException {
    Request req = new Request(db, null, true, null);

    try (MongodSchemaExecutor schemaEx = server.openSchemaExecutor()) {
      return executor.execute(req, command, arg, schemaEx.getDocSchemaExecutor());
    }
  }

  public static class OplogApplyingException extends Exception {

    private static final long serialVersionUID = 5423815920382391458L;

    public OplogApplyingException() {
    }

    public OplogApplyingException(String message) {
      super(message);
    }

    public OplogApplyingException(String message, Throwable cause) {
      super(message, cause);
    }

    public OplogApplyingException(Throwable cause) {
      super(cause);
    }

  }

  @SuppressWarnings("rawtypes")
  private class Visitor implements OplogOperationVisitor<OplogOperationApplierFunction, Void> {

    @Override
    public OplogOperationApplierFunction visit(DbCmdOplogOperation op, Void arg) {
      return (server, applierContext) ->
          applyCmd(op, server);
    }

    @Override
    public OplogOperationApplierFunction visit(DbOplogOperation op, Void arg) {
      return (server, applierContext) -> {
        logger.debug("Ignoring a db operation");
      };
    }

    @Override
    public OplogOperationApplierFunction visit(NoopOplogOperation op, Void arg) {
      return (server, applierContext) -> {
        logger.debug("Ignoring a noop operation");
      };
    }

    @Override
    public OplogOperationApplierFunction visit(DeleteOplogOperation op, Void arg) {
      return (server, context) -> applyCud(op, server, context);
    }

    @Override
    public OplogOperationApplierFunction visit(InsertOplogOperation op, Void arg) {
      BsonDocument docToInsert = op.getDocToInsert();
      if (NamespaceUtil.isIndexesMetaCollection(op.getCollection())) {
        return (server, context) -> {
          insertIndex(docToInsert, op.getDatabase(), server);
        };
      } else {
        return (server, context) -> applyCud(op, server, context);
      }
    }

    @Override
    public OplogOperationApplierFunction visit(UpdateOplogOperation op, Void arg) {
      return (server, context) -> applyCud(op, server, context);
    }
  }

  private void applyCud(CollectionOplogOperation op, MongodServer server, ApplierContext context)
      throws RollbackException, OplogApplyingException {
    AnalyzedOp analyzed = analyzedOpReducer.analyze(op, context);
    NamespaceJob job = new NamespaceJob(
        op.getDatabase(), op.getDatabase(), Collections.singleton(analyzed)
    );
    try {
      namespaceJobExecutor.apply(job, server, true);
    } catch (UserException | NamespaceJobExecutionException ex) {
      throw new OplogApplyingException(ex);
    }
  }

  private void insertIndex(BsonDocument indexDoc, String database,
      MongodServer server) throws OplogApplyingException {
    try {
      CreateIndexesCommand command = CreateIndexesCommand.INSTANCE;
      IndexOptions indexOptions = IndexOptions.unmarshall(indexDoc);

      CreateIndexesArgument arg = new CreateIndexesArgument(
          indexOptions.getCollection(),
          Collections.singletonList(indexOptions)
      );
      Status executionResult = executeReplCommand(database, command, arg, server);
      if (!executionResult.isOk()) {
        throw new OplogApplyingException(new MongoException(executionResult));
      }
    } catch (MongoException ex) {
      throw new OplogApplyingException(ex);
    } catch (TimeoutException ex) {
      throw new OplogApplyingException(ex);
    }
  }

  @SuppressWarnings("unchecked")
  private void applyCmd(DbCmdOplogOperation op, MongodServer server) throws OplogApplyingException {

    LibraryEntry librayEntry = library.find(op.getRequest());

    if (librayEntry == null) {
      throw new OplogApplyingException(new CommandNotFoundException(
          op.getRequest().isEmpty() ? "?" : op.getRequest().getFirstEntry().getKey()));
    }

    Command command = librayEntry.getCommand();
    if (command == null) {
      BsonDocument document = op.getRequest();
      if (document.isEmpty()) {
        throw new OplogApplyingException(new CommandNotFoundException("Empty document query"));
      }
      String firstKey = document.getFirstEntry().getKey();
      throw new OplogApplyingException(new CommandNotFoundException(firstKey));
    }
    Object arg;
    try {
      arg = command.unmarshallArg(op.getRequest(), librayEntry.getAlias());
    } catch (MongoException ex) {
      throw new OplogApplyingException(ex);
    }

    Status executionResult;
    try {
      executionResult = executeReplCommand(op.getDatabase(), command, arg, server);
    } catch (TimeoutException ex) {
      throw new OplogApplyingException(ex);
    }
    if (!executionResult.isOk()) {
      throw new OplogApplyingException(new MongoException(executionResult));
    }
  }

  @FunctionalInterface
  private static interface OplogOperationApplierFunction<E extends OplogOperation> {

    public void apply(MongodServer server, ApplierContext applierContext)
        throws OplogApplyingException;
  }
}
