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

package com.torodb.mongodb.repl.oplogreplier.utils;

import com.google.common.net.HostAndPort;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.repl.oplogreplier.ApplierContext;
import com.torodb.mongodb.repl.oplogreplier.OplogApplier;
import com.torodb.mongodb.repl.oplogreplier.fetcher.LimitedOplogFetcher;
import com.torodb.mongodb.repl.oplogreplier.fetcher.OplogFetcher;
import com.torodb.mongowp.commands.oplog.OplogOperation;
import com.torodb.mongowp.commands.pojos.IteratorMongoCursor;

import java.util.stream.Stream;

public class DefaultContext implements Context {

  private final MongodServer server;
  private final OplogApplier oplogApplier;

  public DefaultContext(MongodServer server, OplogApplier oplogApplier) {
    this.server = server;
    this.oplogApplier = oplogApplier;
  }

  @Override
  public MongodServer getMongodServer() {
    return server;
  }

  @Override
  public void apply(Stream<OplogOperation> streamOplog,
      ApplierContext applierContext) throws Exception {
    oplogApplier.apply(createOplogFetcher(streamOplog), applierContext)
        .waitUntilFinished();
  }

  private OplogFetcher createOplogFetcher(Stream<OplogOperation> opsStream) {
    return new LimitedOplogFetcher(
        new IteratorMongoCursor<>(
            "local",
            "oplog.rs",
            1,
            HostAndPort.fromParts("localhost", 27017),
            opsStream.iterator())
    );
  }

}
