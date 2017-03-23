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

package com.torodb.mongodb.commands.signatures.admin;

import com.torodb.mongowp.bson.BsonDocument;
import com.torodb.mongowp.commands.impl.AbstractNotAliasableCommand;
import com.torodb.mongowp.commands.impl.CollectionCommandArgument;
import com.torodb.mongowp.commands.tools.Empty;
import com.torodb.mongowp.exceptions.BadValueException;
import com.torodb.mongowp.exceptions.NoSuchKeyException;
import com.torodb.mongowp.exceptions.TypesMismatchException;

/**
 *
 */
public class DropCollectionCommand
    extends AbstractNotAliasableCommand<CollectionCommandArgument, Empty> {

  public static final DropCollectionCommand INSTANCE = new DropCollectionCommand();

  private DropCollectionCommand() {
    super("drop");
  }

  @Override
  public Class<? extends CollectionCommandArgument> getArgClass() {
    return CollectionCommandArgument.class;
  }

  @Override
  public CollectionCommandArgument unmarshallArg(BsonDocument requestDoc)
      throws TypesMismatchException, NoSuchKeyException, BadValueException {
    return CollectionCommandArgument.unmarshall(requestDoc, this);
  }

  @Override
  public BsonDocument marshallArg(CollectionCommandArgument request) {
    return request.marshall();
  }

  @Override
  public Class<? extends Empty> getResultClass() {
    return Empty.class;
  }

  @Override
  public BsonDocument marshallResult(Empty reply) {
    return null;
  }

  @Override
  public Empty unmarshallResult(BsonDocument replyDoc) {
    return Empty.getInstance();
  }

}
