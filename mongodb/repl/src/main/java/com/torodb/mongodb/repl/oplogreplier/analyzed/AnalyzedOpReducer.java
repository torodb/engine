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

package com.torodb.mongodb.repl.oplogreplier.analyzed;

import com.google.common.base.Preconditions;
import com.torodb.kvdocument.conversion.mongowp.MongoWpConverter;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.mongodb.repl.oplogreplier.ApplierContext;
import com.torodb.mongowp.bson.BsonValue;
import com.torodb.mongowp.commands.oplog.CollectionOplogOperation;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import javax.annotation.Nullable;

/**
 *
 */
public class AnalyzedOpReducer {

  private final boolean onDebug;

  public AnalyzedOpReducer(boolean onDebug) {
    this.onDebug = onDebug;
  }

  public Map<BsonValue<?>, AnalyzedOp> analyzeAndReduce(
      Stream<CollectionOplogOperation> ops, ApplierContext context) {
    HashMap<BsonValue<?>, AnalyzedOp> map = new HashMap<>();

    ops.forEach(op -> {
      analyzeAndReduce(map, op, context);
    });

    return map;
  }

  public void analyzeAndReduce(Map<BsonValue<?>, AnalyzedOp> map,
      CollectionOplogOperation op, ApplierContext context) {
    Preconditions.checkArgument(op.getDocId() != null,
        "Modifications without _id cannot be replicated in parallel");

    AnalyzedOp oldOp = map.get(op.getDocId());
    map.put(op.getDocId(), analyze(op, context, oldOp));
  }

  public AnalyzedOp analyze(CollectionOplogOperation op, ApplierContext context) {
    return analyze(op, context, null);
  }

  private AnalyzedOp analyze(CollectionOplogOperation op, ApplierContext context, 
      @Nullable AnalyzedOp oldOp) {
    if (oldOp == null) {
      KvValue<?> translated = MongoWpConverter.translate(op.getDocId());
      if (onDebug) {
        oldOp = new DebuggingAnalyzedOp(translated);
      } else {
        oldOp = new NoopAnalyzedOp(translated);
      }
    }
    return oldOp.apply(op, context);
  }
}
