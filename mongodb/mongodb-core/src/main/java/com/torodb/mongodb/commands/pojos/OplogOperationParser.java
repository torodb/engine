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

package com.torodb.mongodb.commands.pojos;

import com.torodb.mongowp.OpTime;
import com.torodb.mongowp.bson.BsonDocument;
import com.torodb.mongowp.bson.BsonInt64;
import com.torodb.mongowp.bson.BsonTimestamp;
import com.torodb.mongowp.bson.BsonValue;
import com.torodb.mongowp.bson.utils.DefaultBsonValues;
import com.torodb.mongowp.commands.oplog.DbCmdOplogOperation;
import com.torodb.mongowp.commands.oplog.DbOplogOperation;
import com.torodb.mongowp.commands.oplog.DeleteOplogOperation;
import com.torodb.mongowp.commands.oplog.InsertOplogOperation;
import com.torodb.mongowp.commands.oplog.NoopOplogOperation;
import com.torodb.mongowp.commands.oplog.OplogOperation;
import com.torodb.mongowp.commands.oplog.OplogOperationType;
import com.torodb.mongowp.commands.oplog.OplogVersion;
import com.torodb.mongowp.commands.oplog.UpdateOplogOperation;
import com.torodb.mongowp.exceptions.BadValueException;
import com.torodb.mongowp.exceptions.NoSuchKeyException;
import com.torodb.mongowp.exceptions.TypesMismatchException;
import com.torodb.mongowp.fields.LongField;
import com.torodb.mongowp.fields.TimestampField;
import com.torodb.mongowp.utils.BsonReaderTool;

import java.util.Locale;
import java.util.function.Function;

import javax.annotation.Nonnull;

public class OplogOperationParser {

  private static final TimestampField TIMESTAMP_FIELD = new TimestampField("ts");
  private static final LongField TERM_FIELD = new LongField("t");
  private static final BsonInt64 UNINITIALIZED_TERM_BSON = DefaultBsonValues.newLong(-1);

  private OplogOperationParser() {
  }

  public static Function<BsonDocument, OplogOperation> asFunction() {
    return AsFunction.INSTANCE;
  }

  public static OplogOperation fromBson(@Nonnull BsonValue uncastedOp) throws
      BadValueException, TypesMismatchException, NoSuchKeyException {
    if (!uncastedOp.isDocument()) {
      throw new BadValueException("found a "
          + uncastedOp.getType().toString().toLowerCase(Locale.ROOT)
          + " where a document that represents a oplog operation "
          + "was expected");
    }
    BsonDocument doc = uncastedOp.asDocument();

    OplogOperationType opType;
    String opString = BsonReaderTool.getString(doc, "op");
    try {
      opType = OplogOperationType.fromOplogName(opString);
    } catch (IllegalArgumentException ex) {
      throw new BadValueException("Unknown oplog operation with type '" + opString + "'");
    }

    String ns;
    try {
      ns = BsonReaderTool.getString(doc, "ns");
    } catch (NoSuchKeyException ex) {
      throw new NoSuchKeyException("ns", "op does not contain required \"ns\" field: "
          + uncastedOp);
    } catch (TypesMismatchException ex) {
      throw ex.newWithMessage("\"ns\" field is not a string: " + uncastedOp);
    }

    if (ns.isEmpty() && !opType.equals(OplogOperationType.NOOP)) {
      throw new BadValueException("\"ns\" field value cannot be empty "
          + "when op type is not 'n': " + doc);
    }
    String db;
    String collection;
    int firstDotIndex = ns.indexOf('.');
    if (firstDotIndex == -1 || firstDotIndex + 1 == ns.length()) {
      db = ns;
      collection = null;
    } else {
      db = ns.substring(0, firstDotIndex);
      collection = ns.substring(firstDotIndex + 1);
    }

    OpTime optime = fromOplogEntry(doc);
    long h = BsonReaderTool.getLong(doc, "h");
    OplogVersion version = OplogVersion.valueOf(BsonReaderTool.getInteger(doc, "v"));
    //Note: Mongodb v3 checks if the key exists or not, but doesn't check the value
    boolean fromMigrate = doc.containsKey("fromMigrate");
    BsonDocument o = BsonReaderTool.getDocument(doc, "o");

    switch (opType) {
      case DB:
        return new DbOplogOperation(
            db,
            optime,
            h,
            version,
            fromMigrate
        );
      case DB_CMD:
        return new DbCmdOplogOperation(
            o,
            db,
            optime,
            h,
            version,
            fromMigrate
        );
      case DELETE:
        return new DeleteOplogOperation(
            o,
            db,
            collection,
            optime,
            h,
            version,
            fromMigrate,
            BsonReaderTool.getBoolean(doc, "b", false)
        );
      case INSERT:
        //TODO: parse b
        return new InsertOplogOperation(
            o,
            db,
            collection,
            optime,
            h,
            version,
            fromMigrate
        );
      case NOOP:
        return new NoopOplogOperation(o, db, optime, h, version, fromMigrate);
      case UPDATE:
        return new UpdateOplogOperation(
            BsonReaderTool.getDocument(doc, "o2"),
            db,
            collection,
            optime,
            h,
            version,
            fromMigrate,
            o,
            BsonReaderTool.getBoolean(doc, "b", false)
        );
      default:
        throw new AssertionError(OplogOperationParser.class
            + " is not prepared to work with oplog operations of type " + opType);
    }
  }

  @Nonnull
  private static OpTime fromOplogEntry(BsonDocument doc) throws TypesMismatchException,
      NoSuchKeyException {
    BsonTimestamp ts = BsonReaderTool.getTimestamp(doc, TIMESTAMP_FIELD);
    //TODO(gortiz): check precision lost
    long term = BsonReaderTool.getNumeric(doc, TERM_FIELD, UNINITIALIZED_TERM_BSON)
        .getValue()
        .longValue();
    if (term == UNINITIALIZED_TERM_BSON.longValue()) {
      return new OpTime(ts);
    } else {
      return new OpTime(ts, term);
    }
  }

  private static class AsFunction implements Function<BsonDocument, OplogOperation> {

    private static final AsFunction INSTANCE = new AsFunction();

    @Override
    public OplogOperation apply(BsonDocument input) {
      if (input == null) {
        return null;
      }
      try {
        return fromBson(input);
      } catch (BadValueException ex) {
        throw new IllegalArgumentException(ex);
      } catch (TypesMismatchException ex) {
        throw new IllegalArgumentException(ex);
      } catch (NoSuchKeyException ex) {
        throw new IllegalArgumentException(ex);
      }
    }

  }
}
