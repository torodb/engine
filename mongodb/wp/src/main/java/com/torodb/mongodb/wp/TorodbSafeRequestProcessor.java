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

package com.torodb.mongodb.wp;

import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.mongodb.commands.CommandClassifier;
import com.torodb.mongodb.commands.RequiredTransaction;
import com.torodb.mongodb.commands.signatures.general.FindCommand;
import com.torodb.mongodb.commands.signatures.general.FindCommand.FindArgument;
import com.torodb.mongodb.commands.signatures.general.FindCommand.FindResult;
import com.torodb.mongodb.core.MongodMetrics;
import com.torodb.mongodb.core.MongodSchemaExecutor;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.MongodTransaction;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongowp.ErrorCode;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.bson.utils.DefaultBsonValues;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.CommandLibrary;
import com.torodb.mongowp.commands.Request;
import com.torodb.mongowp.commands.SafeRequestProcessor;
import com.torodb.mongowp.commands.pojos.QueryRequest;
import com.torodb.mongowp.exceptions.MongoException;
import com.torodb.mongowp.messages.request.DeleteMessage;
import com.torodb.mongowp.messages.request.EmptyBsonContext;
import com.torodb.mongowp.messages.request.GetMoreMessage;
import com.torodb.mongowp.messages.request.InsertMessage;
import com.torodb.mongowp.messages.request.KillCursorsMessage;
import com.torodb.mongowp.messages.request.UpdateMessage;
import com.torodb.mongowp.messages.response.ReplyMessage;
import com.torodb.mongowp.messages.utils.IterableDocumentProvider;
import io.netty.util.AttributeKey;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

@ThreadSafe
public class TorodbSafeRequestProcessor implements SafeRequestProcessor<MongoDbWpConnection> {

  private final Logger logger;
  private final MongodServer server;
  public static final AttributeKey<MongoDbWpConnection> MONGOD_CONNECTION_KEY = AttributeKey
      .newInstance("mongod.connection");
  private final Retrier retrier;
  private final CommandLibrary commandLibrary;
  private final CommandClassifier commandClassifier;
  private final MongodMetrics mongodMetrics;
  private final AtomicInteger conIdGenerator = new AtomicInteger();

  @Inject
  public TorodbSafeRequestProcessor(LoggerFactory loggerFactory,MongodServer server,
      Retrier retrier, CommandLibrary commandLibrary, CommandClassifier commandClassifier,
      MongodMetrics mongodMetrics) {
    this.server = server;
    this.retrier = retrier;
    this.commandLibrary = commandLibrary;
    this.commandClassifier = commandClassifier;
    this.mongodMetrics = mongodMetrics;
    this.logger = loggerFactory.apply(this.getClass());
  }

  @Override
  public MongoDbWpConnection openConnection() {
    MongoDbWpConnection connection = new MongoDbWpConnection(conIdGenerator.incrementAndGet());

    logger.info("Accepted connection {}", connection.getConnectionId());

    return connection;
  }

  @Override
  public <A, R> Status<R> execute(Request req, Command<? super A, ? super R> command,
      A arg, MongoDbWpConnection connection) {
    mongodMetrics.getCommands().mark();
    Timer timer = mongodMetrics.getTimer(command);
    try (Timer.Context ctx = timer.time()) {
      Callable<Status<R>> callable;
      RequiredTransaction commandType = commandClassifier.classify(command);
      switch (commandType) {
        case NO_TRANSACTION:
          callable = () -> {
            return server.execute(req, command, arg);
          };
          break;
        case READ_TRANSACTION:
          callable = () -> {
            try (MongodTransaction trans = server.openReadTransaction()) {
              return trans.execute(req, command, arg);
            }
          };
          break;
        case WRITE_TRANSACTION:
          callable = () -> {
            try (WriteMongodTransaction trans = server.openWriteTransaction()) {
              Status<R> result = trans.execute(req, command, arg);
              if (result.isOk()) {
                trans.commit();
              }
              return result;
            }
          };
          break;
        case EXCLUSIVE_TRANSACTION:
          callable = () -> {
            try (MongodSchemaExecutor schemaEx = server.openSchemaExecutor()) {
              return schemaEx.execute(req, command, arg);
            }
          };
          break;
        default:
          throw new AssertionError("Unexpected command type" + commandType);
      }

      try {
        return retrier.retry(callable);
      } catch (RetrierGiveUpException ex) {
        return Status.from(
            ErrorCode.CONFLICTING_OPERATION_IN_PROGRESS,
            "It was impossible to execute " + command.getCommandName() + " after several attempts"
        );
      }
    }
  }

  @Override
  public CommandLibrary getCommandsLibrary() {
    return commandLibrary;
  }

  @Override
  public ReplyMessage query(MongoDbWpConnection connection, Request req, int requestId,
      QueryRequest queryRequest) throws
      MongoException {

    FindArgument findArg = new FindArgument.Builder()
        .setCollection(queryRequest.getCollection())
        .setFilter(queryRequest.getQuery() != null ? queryRequest.getQuery() :
            DefaultBsonValues.EMPTY_DOC)
        .build();

    Status<FindResult> status = execute(req, FindCommand.INSTANCE, findArg, connection);

    if (!status.isOk()) {
      throw new MongoException(status.getErrorCode(), status.getErrorMsg());
    }

    FindResult result = status.getResult();
    assert result != null;

    return new ReplyMessage(
        EmptyBsonContext.getInstance(),
        requestId,
        false,
        false,
        false,
        false,
        result.getCursor().getCursorId(),
        queryRequest.getNumberToSkip(),
        IterableDocumentProvider.of(Lists.newArrayList(result.getCursor().getFirstBatch()))
    );
  }

  @Override
  public ReplyMessage getMore(MongoDbWpConnection connection, Request req, int requestId,
      GetMoreMessage moreMessage)
      throws MongoException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void killCursors(MongoDbWpConnection connection, Request req,
      KillCursorsMessage killCursorsMessage)
      throws MongoException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void insert(MongoDbWpConnection connection, Request req, InsertMessage insertMessage)
      throws MongoException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(MongoDbWpConnection connection, Request req, UpdateMessage updateMessage)
      throws MongoException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void delete(MongoDbWpConnection connection, Request req, DeleteMessage deleteMessage)
      throws MongoException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
