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

import com.torodb.mongodb.commands.pojos.OplogOperationParser;
import com.torodb.mongowp.bson.BsonDocument;
import com.torodb.mongowp.bson.BsonInt32;
import com.torodb.mongowp.bson.BsonValue;
import com.torodb.mongowp.bson.utils.DefaultBsonValues;
import com.torodb.mongowp.commands.oplog.OplogOperation;
import com.torodb.mongowp.exceptions.MongoException;
import com.torodb.mongowp.utils.BsonDocumentBuilder;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 *
 */
public class OplogOpsParser {

  private static AtomicInteger TS_FACTORY = new AtomicInteger();
  private static AtomicInteger T_FACTORY = new AtomicInteger();

  private OplogOpsParser() {}

  public static List<OplogOperation> parseOps(BsonDocument doc) {
    BsonValue<?> oplogValue = doc.get("oplog");
    if (oplogValue == null) {
      throw new AssertionError("Does not contain oplog");
    }
    BsonInt32 twoInt32 = DefaultBsonValues.newInt(2);

    return oplogValue.asArray().asList().stream()
        .map(BsonValue::asDocument)
        .map(child -> {
          BsonDocumentBuilder builder = new BsonDocumentBuilder(child);
          if (child.get("ts") == null) {
            builder.appendUnsafe("ts", DefaultBsonValues.newTimestamp(
                TS_FACTORY.incrementAndGet(),
                T_FACTORY.incrementAndGet())
            );
          }
          if (child.get("h") == null) {
            builder.appendUnsafe("h", DefaultBsonValues.INT32_ONE);
          }
          if (child.get("v") == null) {
            builder.appendUnsafe("v", twoInt32);
          }
          return builder.build();
        })
        .map(child -> {
          try {
            return OplogOperationParser.fromBson(child);
          } catch (MongoException ex) {
            throw new AssertionError("Invalid oplog operation", ex);
          }
        })
        .collect(Collectors.toList());
  }
}
