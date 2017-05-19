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
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.mongodb.commands.impl.RetrierSchemaCommandImpl;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.Request;
import com.torodb.mongowp.commands.tools.Empty;
import com.torodb.torod.SchemaOperationExecutor;


public class DropDatabaseImplementation extends RetrierSchemaCommandImpl<Empty, Empty> {

  public DropDatabaseImplementation(LoggerFactory loggerFactory) {
    super(loggerFactory);
  }

  @Override
  protected Status<Empty> tryApply(Request req,
      Command<? super Empty, ? super Empty> command, Empty arg, SchemaOperationExecutor context)
      throws RetrierGiveUpException, RollbackException {
    getLogger().info("Drop database {}", req.getDatabase());

    context.dropDatabase(req.getDatabase());

    return Status.ok();
  }

  @Override
  protected String getErrorMessage(Request req, Command<? super Empty, ? super Empty> command,
      Empty arg) {
    return "Catching an exception while droping the database " + req.getDatabase();
  }

}
