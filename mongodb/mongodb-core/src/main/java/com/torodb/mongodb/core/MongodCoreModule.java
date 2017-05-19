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

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.commands.CommandClassifier;
import com.torodb.mongodb.core.MongodServer.MongodSchemaExecutorFactory;
import com.torodb.mongodb.core.MongodServer.ReadMongodTransactionFactory;
import com.torodb.mongodb.core.MongodServer.WriteMongodTransactionFactory;
import com.torodb.mongodb.language.ObjectIdFactory;
import com.torodb.mongowp.commands.CommandLibrary;
import com.torodb.torod.TorodServer;

public class MongodCoreModule extends PrivateModule {

  private final MongoDbCoreConfig config;

  public MongodCoreModule(MongoDbCoreConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    expose(MongodServer.class);
    
    bind(MongodServer.class)
        .in(Singleton.class);

    bind(CommandLibrary.class)
        .toInstance(config.getCommandsLibrary());
    bind(CommandClassifier.class)
        .toInstance(config.getCommandClassifier());
    bind(TorodServer.class)
        .toInstance(config.getTorodBundle().getExternalInterface().getTorodServer());

    bind(ObjectIdFactory.class)
        .in(Singleton.class);

    bind(MongodMetrics.class)
        .in(Singleton.class);

    bind(Supervisor.class)
        .toInstance(config.getSupervisor());
  }

  @Provides
  ReadMongodTransactionFactory createReadMongodTransactionFactory(LoggerFactory loggerFactory,
      CommandClassifier commandClassifier) {
    return (docTrans) -> new ReadOnlyMongodTransactionImpl(
        loggerFactory,
        docTrans,
        commandClassifier
    );
  }

  @Provides
  WriteMongodTransactionFactory createWriteMongodTransactionFactory(LoggerFactory loggerFactory,
      CommandClassifier commandClassifier, ObjectIdFactory objectIdFactory, MongodMetrics metrics) {
    return (docTrans) -> new WriteMongodTransactionImpl(
        loggerFactory,
        docTrans,
        commandClassifier,
        objectIdFactory,
        metrics
    );
  }

  @Provides
  MongodSchemaExecutorFactory createMongodSchemaExecutorFactory(
      CommandClassifier commandClassifier) {
    return (torodSchemaOperationEx) -> new MongodSchemaExecutor(
        commandClassifier.getSchemaCommandsExecutor(),
        torodSchemaOperationEx
    );
  }
}
