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

package com.torodb.mongodb.core;

import com.google.common.base.Preconditions;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.CommandExecutor;
import com.torodb.mongowp.commands.Request;
import com.torodb.torod.SchemaOperationExecutor;

/**
 *
 */
public class MongodSchemaExecutor implements AutoCloseable {

  private final CommandExecutor<? super SchemaOperationExecutor> commandsExecutor;
  private final SchemaOperationExecutor schemaOperationExecutor;

  public MongodSchemaExecutor(
      CommandExecutor<? super SchemaOperationExecutor> commandsExecutor,
      SchemaOperationExecutor schemaOperationExecutor) {
    this.commandsExecutor = commandsExecutor;
    this.schemaOperationExecutor = schemaOperationExecutor;
  }

  public <A, R> Status<R> execute(Request request, Command<? super A, ? super R> command, A arg) {
    Preconditions.checkState(!schemaOperationExecutor.isClosed(), "This executor is closed");
    return commandsExecutor.execute(request, command, arg, schemaOperationExecutor);
  }

  public SchemaOperationExecutor getDocSchemaExecutor() {
    return schemaOperationExecutor;
  }

  @Override
  public void close() {
    schemaOperationExecutor.close();
  }

}
