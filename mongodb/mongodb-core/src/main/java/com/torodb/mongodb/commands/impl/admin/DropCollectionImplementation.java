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

package com.torodb.mongodb.commands.impl.admin;

import com.torodb.core.logging.LoggerFactory;
import com.torodb.mongodb.commands.impl.RetrierSchemaCommandImpl;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.Request;
import com.torodb.mongowp.commands.impl.CollectionCommandArgument;
import com.torodb.mongowp.commands.tools.Empty;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.exception.UnexistentDatabaseException;


public class DropCollectionImplementation 
    extends RetrierSchemaCommandImpl<CollectionCommandArgument, Empty> {

  public DropCollectionImplementation(LoggerFactory loggerFactory) {
    super(loggerFactory);
  }

  @Override
  public Status<Empty> tryApply(Request req,
      Command<? super CollectionCommandArgument, ? super Empty> command,
      CollectionCommandArgument arg, SchemaOperationExecutor context) {
    try {
      logDropCommand(req, arg);

      context.dropCollection(req.getDatabase(), arg.getCollection());
    } catch (UnexistentDatabaseException ex) {
      getLogger().debug("Trying to drop {}.{} when database {} doesn't exist",
          req.getDatabase(),
          arg.getCollection(),
          req.getDatabase()
      );
    } 

    return Status.ok();
  }

  private void logDropCommand(Request req, CollectionCommandArgument arg) {
    String collection = arg.getCollection();

    getLogger().info("Drop collection {}.{}", req.getDatabase(), collection);
  }

}
