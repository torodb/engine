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

package com.torodb.mongodb.commands.signatures.repl;

import com.torodb.mongodb.commands.pojos.ReplicaSetConfig;
import com.torodb.mongodb.commands.tools.EmptyCommandArgumentMarshaller;
import com.torodb.mongowp.MongoConstants;
import com.torodb.mongowp.bson.BsonDocument;
import com.torodb.mongowp.bson.utils.DefaultBsonValues;
import com.torodb.mongowp.commands.impl.AbstractNotAliasableCommand;
import com.torodb.mongowp.commands.tools.Empty;
import com.torodb.mongowp.exceptions.BadValueException;
import com.torodb.mongowp.exceptions.MongoException;
import com.torodb.mongowp.fields.DocField;
import com.torodb.mongowp.utils.BsonReaderTool;

/**
 * Returns the current replica set configuration.
 */
public class ReplSetGetConfigCommand extends AbstractNotAliasableCommand<Empty, ReplicaSetConfig> {

  public static final ReplSetGetConfigCommand INSTANCE = new ReplSetGetConfigCommand();
  private static final DocField CONFIG_FIELD = new DocField("config");

  private ReplSetGetConfigCommand() {
    super("replSetGetConfig");
  }

  @Override
  public Class<? extends Empty> getArgClass() {
    return Empty.class;
  }

  @Override
  public Empty unmarshallArg(BsonDocument requestDoc) {
    return Empty.getInstance();
  }

  @Override
  public BsonDocument marshallArg(Empty request) {
    return EmptyCommandArgumentMarshaller.marshallEmptyArgument(this);
  }

  @Override
  public Class<? extends ReplicaSetConfig> getResultClass() {
    return ReplicaSetConfig.class;
  }

  @Override
  public BsonDocument marshallResult(ReplicaSetConfig reply) {
    return DefaultBsonValues.newDocument("config", reply.toBson());
  }

  @Override
  public ReplicaSetConfig unmarshallResult(BsonDocument resultDoc) throws
      MongoException, UnsupportedOperationException {
    if (!resultDoc.get("ok").equals(MongoConstants.BSON_OK)) {
      throw new BadValueException("It is not defined how to parse errors "
          + "from " + getCommandName());
    }
    return ReplicaSetConfig.fromDocument(
        BsonReaderTool.getDocument(resultDoc, CONFIG_FIELD)
    );
  }

}
