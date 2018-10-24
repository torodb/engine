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

import com.torodb.mongodb.commands.CommandClassifier;
import com.torodb.mongowp.commands.CommandLibrary;

/**
 * The external interface provided by a {@link MongoDbCoreBundle}.
 */
public class MongoDbCoreExtInt {

  private final MongodServer mongodServer;
  private final CommandLibrary commandLibrary;
  private final CommandClassifier commandClassifier;

  public MongoDbCoreExtInt(MongodServer mongodServer, CommandLibrary commandLibrary,
      CommandClassifier commandClassifier) {
    this.mongodServer = mongodServer;
    this.commandLibrary = commandLibrary;
    this.commandClassifier = commandClassifier;
  }

  public MongodServer getMongodServer() {
    return mongodServer;
  }

  public CommandLibrary getCommandLibrary() {
    return commandLibrary;
  }

  public CommandClassifier getCommandClassifier() {
    return commandClassifier;
  }
}
