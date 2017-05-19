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

import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.mongodb.commands.impl.RetrierSchemaCommandImpl;
import com.torodb.mongodb.commands.signatures.admin.RenameCollectionCommand.RenameCollectionArgument;
import com.torodb.mongowp.ErrorCode;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.Request;
import com.torodb.mongowp.commands.tools.Empty;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.exception.AlreadyExistentCollectionException;
import com.torodb.torod.exception.UnexistentCollectionException;
import com.torodb.torod.exception.UnexistentDatabaseException;

public class RenameCollectionImplementation 
    extends RetrierSchemaCommandImpl<RenameCollectionArgument, Empty> {

  public RenameCollectionImplementation(LoggerFactory loggerFactory) {
    super(loggerFactory);
  }

  @Override
  public Status<Empty> tryApply(Request req,
      Command<? super RenameCollectionArgument, ? super Empty> command,
      RenameCollectionArgument arg, SchemaOperationExecutor context) {
    try {
      if (arg.isDropTarget()) {
        context.dropCollection(arg.getToDatabase(), arg.getToCollection());
      }

      context.renameCollection(arg.getFromDatabase(), arg.getFromCollection(),
          arg.getToDatabase(), arg.getToCollection());
    } catch (AlreadyExistentCollectionException ex) {
      return Status.from(ErrorCode.COMMAND_FAILED, "Target collection " + arg.getToDatabase() 
          + "." + arg.getToCollection() + " already exist. Try to rename the source collection "
          + "with 'dropTarget' option");
    } catch (UnexistentCollectionException ex) {
      return Status.from(ErrorCode.COMMAND_FAILED, "Source collection " + arg.getFromDatabase()
          + "." + arg.getFromCollection() + " doesn't exist");
    } catch (UnexistentDatabaseException ex) {
      return Status.from(ErrorCode.COMMAND_FAILED, "Source database " + arg.getFromDatabase()
          + " doesn't exist");
    } catch (UserException ex) {
      return Status.from(ErrorCode.COMMAND_FAILED, ex.getLocalizedMessage());
    }

    return Status.ok();
  }

}
