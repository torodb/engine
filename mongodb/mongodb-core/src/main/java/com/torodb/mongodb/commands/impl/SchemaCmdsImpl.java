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

package com.torodb.mongodb.commands.impl;

import com.google.common.collect.ImmutableMap;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.mongodb.commands.CmdImplMapSupplier;
import com.torodb.mongodb.commands.impl.admin.CreateCollectionImplementation;
import com.torodb.mongodb.commands.impl.admin.CreateIndexesImplementation;
import com.torodb.mongodb.commands.impl.admin.DropCollectionImplementation;
import com.torodb.mongodb.commands.impl.admin.DropDatabaseImplementation;
import com.torodb.mongodb.commands.impl.admin.DropIndexesImplementation;
import com.torodb.mongodb.commands.impl.admin.RenameCollectionImplementation;
import com.torodb.mongodb.commands.signatures.admin.CreateCollectionCommand;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand;
import com.torodb.mongodb.commands.signatures.admin.DropCollectionCommand;
import com.torodb.mongodb.commands.signatures.admin.DropDatabaseCommand;
import com.torodb.mongodb.commands.signatures.admin.DropIndexesCommand;
import com.torodb.mongodb.commands.signatures.admin.RenameCollectionCommand;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.CommandImplementation;
import com.torodb.torod.SchemaOperationExecutor;

import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
@SuppressWarnings("checkstyle:LineLength")
public class SchemaCmdsImpl implements CmdImplMapSupplier<SchemaOperationExecutor> {

  private final Map<Command<?, ?>, CommandImplementation<?, ?, ? super SchemaOperationExecutor>> map;

  @Inject
  SchemaCmdsImpl(LoggerFactory loggerFactory) {
    this.map = ImmutableMap.<Command<?, ?>, CommandImplementation<?, ?, ? super SchemaOperationExecutor>>builder()
        .put(CreateCollectionCommand.INSTANCE, new CreateCollectionImplementation(loggerFactory))
        .put(CreateIndexesCommand.INSTANCE, new CreateIndexesImplementation(loggerFactory))
        .put(DropCollectionCommand.INSTANCE, new DropCollectionImplementation(loggerFactory))
        .put(DropDatabaseCommand.INSTANCE, new DropDatabaseImplementation(loggerFactory))
        .put(DropIndexesCommand.INSTANCE, new DropIndexesImplementation(loggerFactory))
        .put(RenameCollectionCommand.INSTANCE, new RenameCollectionImplementation(loggerFactory))
        .build();
  }

  @DoNotChange
  Set<Command<?, ?>> getSupportedCommands() {
    return map.keySet();
  }

  @Override
  public Map<Command<?, ?>, CommandImplementation<?, ?, ? super SchemaOperationExecutor>> get() {
    return map;
  }
}