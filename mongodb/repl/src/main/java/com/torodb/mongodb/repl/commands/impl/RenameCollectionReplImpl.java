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

package com.torodb.mongodb.repl.commands.impl;

import com.google.inject.Inject;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.mongodb.commands.signatures.admin.RenameCollectionCommand.RenameCollectionArgument;
import com.torodb.mongodb.filters.NamespaceFilter;
import com.torodb.mongowp.ErrorCode;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.Request;
import com.torodb.mongowp.commands.tools.Empty;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.exception.AlreadyExistentCollectionException;
import com.torodb.torod.exception.UnexistentCollectionException;
import com.torodb.torod.exception.UnexistentDatabaseException;
import org.apache.logging.log4j.Logger;

public class RenameCollectionReplImpl
    extends ExclusiveReplCommandImpl<RenameCollectionArgument, Empty> {

  private final Logger logger;
  private final NamespaceFilter namespaceFilter;

  @Inject
  public RenameCollectionReplImpl(NamespaceFilter namespaceFilter, LoggerFactory loggerFactory) {
    this.namespaceFilter = namespaceFilter;
    this.logger = loggerFactory.apply(this.getClass());
  }

  @Override
  public Status<Empty> apply(Request req,
      Command<? super RenameCollectionArgument, ? super Empty> command,
      RenameCollectionArgument arg, SchemaOperationExecutor schemaEx) {
    try {
      if (!namespaceFilter.filter(arg.getToDatabase(), arg.getToCollection())) {
        if (!namespaceFilter.filter(arg.getFromDatabase(), arg.getFromCollection())) {
          logger.info("Skipping rename operation for filtered source collection {}.{} "
              + "and filtered target collection {}.{}.",
              arg.getFromDatabase(), arg.getFromCollection(),
              arg.getToDatabase(), arg.getToCollection());
          return Status.ok();
        }

        logger.info("Can not rename operation for filtered target collection {}.{}. "
            + "Dropping source collection {}.{}.",
            arg.getToDatabase(), arg.getToCollection(),
            arg.getFromDatabase(), arg.getFromCollection());
        try {
          schemaEx.dropCollection(arg.getFromDatabase(), arg.getFromCollection());
          return Status.ok();
        } catch (UnexistentDatabaseException ex) {
          logger.info("Trying to drop collection {}.{} but it has not been found. "
              + "This is normal when reapplying oplog during a recovery. Ignoring operation",
              arg.getFromDatabase(), arg.getFromCollection());
          return Status.ok(Empty.getInstance());
        }
      }

      if (arg.isDropTarget()) {
        try {
          schemaEx.dropCollection(arg.getToDatabase(), arg.getToCollection());
        } catch (UnexistentDatabaseException ex) {
          logger.info("Trying to drop collection {}.{} but it has not been found. "
              + "This is normal when reapplying oplog during a recovery. Skipping operation",
              arg.getToDatabase(), arg.getToCollection());
        }
      }

      if (!namespaceFilter.filter(arg.getFromDatabase(), arg.getFromCollection())) {
        logger.info("Skipping rename operation for filtered source collection {}.{}.",
            arg.getFromDatabase(), arg.getFromCollection());
        return Status.ok();
      }

      logger.info("Renaming collection {}.{} to {}.{}", arg.getFromDatabase(), arg
          .getFromCollection(),
          arg.getToDatabase(), arg.getToCollection());

      schemaEx.renameCollection(arg.getFromDatabase(), arg.getFromCollection(),
          arg.getToDatabase(), arg.getToCollection());
    } catch (UserException ex) {
      return Status.from(ErrorCode.COMMAND_FAILED, ex.getLocalizedMessage());
    } catch (AlreadyExistentCollectionException | UnexistentCollectionException 
        | UnexistentDatabaseException ex) {
      logger.info("Trying to rename collection {}.{} to {}.{} but it {} was thrown. This is "
          + "normal when reapplying oplog during a recovery.",
          arg.getFromDatabase(), arg.getFromCollection(), arg.getToDatabase(),
          arg.getToCollection(),ex.getClass().getSimpleName());
    }

    return Status.ok();
  }

}
