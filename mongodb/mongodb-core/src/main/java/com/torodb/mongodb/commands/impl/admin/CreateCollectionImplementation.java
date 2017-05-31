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
import com.torodb.mongodb.commands.signatures.admin.CreateCollectionCommand.CreateCollectionArgument;
import com.torodb.mongowp.ErrorCode;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.Request;
import com.torodb.mongowp.commands.tools.Empty;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.exception.UserSchemaException;

import java.util.Collections;


public class CreateCollectionImplementation
    extends RetrierSchemaCommandImpl<CreateCollectionArgument, Empty> {

  public CreateCollectionImplementation(LoggerFactory loggerFactory) {
    super(loggerFactory);
  }

  @Override
  public Status<Empty> tryApply(Request req,
      Command<? super CreateCollectionArgument, ? super Empty> command,
      CreateCollectionArgument arg, SchemaOperationExecutor context) {
    try {
      String dbName = req.getDatabase();
      String colName = arg.getCollection();
      context.prepareSchema(dbName, colName, Collections.emptyList());

      return Status.ok();
    } catch (UserSchemaException ex) {
      //TODO: Improve error reporting
      return Status.from(ErrorCode.COMMAND_FAILED, ex.getCause().getLocalizedMessage());
    }
  }

}
