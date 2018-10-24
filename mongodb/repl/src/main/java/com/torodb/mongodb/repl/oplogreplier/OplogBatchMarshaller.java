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

package com.torodb.mongodb.repl.oplogreplier;

import com.torodb.akka.chronicle.queue.Marshaller;
import com.torodb.mongodb.commands.pojos.OplogOperationParser;
import com.torodb.mongodb.repl.oplogreplier.batch.OplogBatch;
import com.torodb.mongowp.bson.BsonArray;
import com.torodb.mongowp.bson.BsonDocument;
import com.torodb.mongowp.bson.BsonValue;
import com.torodb.mongowp.bson.org.bson.utils.JsonBsonDocumentReader;
import com.torodb.mongowp.bson.org.bson.utils.JsonBsonDocumentWriter;
import com.torodb.mongowp.bson.utils.BsonDocumentReader;
import com.torodb.mongowp.bson.utils.BsonDocumentReaderException;
import com.torodb.mongowp.commands.oplog.OplogOperation;
import com.torodb.mongowp.exceptions.TypesMismatchException;
import com.torodb.mongowp.fields.ArrayField;
import com.torodb.mongowp.fields.BooleanField;
import com.torodb.mongowp.utils.BsonArrayBuilder;
import com.torodb.mongowp.utils.BsonDocumentBuilder;
import com.torodb.mongowp.utils.BsonReaderTool;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.UncheckedException;

import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class OplogBatchMarshaller implements Marshaller<OplogBatch> {

  private static final boolean LAST_ONE_DEFAULT_VALUE = false;
  private static final boolean READY_FOR_MORE_DEFAULT_VALUE = true;
  private static final BooleanField LAST_ONE_FIELD = new BooleanField("lastOne");
  private static final BooleanField READY_FOR_MORE_FIELD = new BooleanField("readyForMore");
  private static final ArrayField OPS_FIELD = new ArrayField("ops");
  private static final BsonArray DEFAULT_OPS = new BsonArrayBuilder().build();

  private static final JsonBsonDocumentReader bsonReader = JsonBsonDocumentReader.getInstance();
  private static final JsonBsonDocumentWriter bsonWriter = JsonBsonDocumentWriter.getInstance();

  @Override
  public void write(WireOut wire, OplogBatch batch) {
    BsonDocument batchDoc = translate(batch);

    wire.write().text(bsonWriter.writeIntoString(batchDoc));
  }

  @Override
  public OplogBatch read(WireIn wire) {
    String text = wire.read().text();

    try {
      BsonDocument batchDoc = bsonReader.readDocument(BsonDocumentReader.AllocationType.HEAP, text);
      return translate(batchDoc);
    } catch (BsonDocumentReaderException ex) {
      throw new UncheckedException(ex);
    }
  }

  private static BsonDocument translate(OplogBatch batch) {
    BsonDocumentBuilder builder = new BsonDocumentBuilder();
    if (batch.isLastOne() != LAST_ONE_DEFAULT_VALUE) {
      builder.append(LAST_ONE_FIELD, batch.isLastOne());
    }
    if (batch.isReadyForMore() != READY_FOR_MORE_DEFAULT_VALUE) {
      builder.append(READY_FOR_MORE_FIELD, batch.isReadyForMore());
    }
    if (!batch.getOps().isEmpty()) {
      builder.append(OPS_FIELD, translate(batch.getOps()));
    }
    return builder.build();
  }

  private static OplogBatch translate(BsonDocument doc) {
    try {
      boolean lastOne = BsonReaderTool.getBoolean(doc, LAST_ONE_FIELD, LAST_ONE_DEFAULT_VALUE);
      boolean readyForMore = BsonReaderTool.getBoolean(doc, READY_FOR_MORE_FIELD,
          READY_FOR_MORE_DEFAULT_VALUE);
      List<OplogOperation> ops = BsonReaderTool.getArray(doc, OPS_FIELD, DEFAULT_OPS).stream()
          .map(BsonValue::asDocument)
          .map(Unchecked.function(OplogOperationParser::fromBson))
          .collect(Collectors.toList());
      
      if (lastOne) {
        return FinishedOplogBatch.getInstance();
      }
      if (readyForMore || !ops.isEmpty()) {
        return new NormalOplogBatch(ops, readyForMore);
      } else {
        return NotReadyForMoreOplogBatch.getInstance();
      }
    } catch (TypesMismatchException ex) {
      throw new UncheckedException(ex);
    }
  }
  
  private static BsonArray translate(List<OplogOperation> ops) {
    BsonArrayBuilder arrBuilder = new BsonArrayBuilder(ops.size());
    for (OplogOperation op : ops) {
      arrBuilder.add(op.toDescriptiveBson().build());
    }
    return arrBuilder.build();
  }
}
