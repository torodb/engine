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

import com.torodb.mongowp.bson.BsonDocument;
import com.torodb.mongowp.bson.utils.DefaultBsonValues;
import com.torodb.mongowp.commands.impl.AbstractNotAliasableCommand;
import com.torodb.mongowp.commands.tools.Empty;
import com.torodb.mongowp.exceptions.NoSuchKeyException;
import com.torodb.mongowp.exceptions.TypesMismatchException;
import com.torodb.mongowp.utils.BsonReaderTool;

/**
 *
 */
public class ReplSetMaintenanceCommand extends AbstractNotAliasableCommand<Boolean, Empty> {

  public static final ReplSetMaintenanceCommand INSTANCE = new ReplSetMaintenanceCommand();

  private ReplSetMaintenanceCommand() {
    super("replSetMaintenance");
  }

  @Override
  public Class<? extends Boolean> getArgClass() {
    return Boolean.class;
  }

  @Override
  public boolean canChangeReplicationState() {
    return true;
  }

  @Override
  public Boolean unmarshallArg(BsonDocument requestDoc) throws TypesMismatchException,
      NoSuchKeyException {
    return BsonReaderTool.getBoolean(requestDoc, getCommandName());
  }

  @Override
  public BsonDocument marshallArg(Boolean request) {
    return DefaultBsonValues.newDocument(getCommandName(), DefaultBsonValues.newBoolean(request));
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
  public Empty unmarshallResult(BsonDocument resultDoc) {
    return Empty.getInstance();
  }
}
