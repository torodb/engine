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

package com.torodb.mongodb.commands.impl.general;

import com.torodb.core.exceptions.user.UserException;
import com.torodb.kvdocument.conversion.mongowp.FromBsonValueTranslator;
import com.torodb.kvdocument.conversion.mongowp.MongoWpConverter;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.mongodb.commands.impl.WriteTransactionCommandImpl;
import com.torodb.mongodb.commands.signatures.general.InsertCommand.InsertArgument;
import com.torodb.mongodb.commands.signatures.general.InsertCommand.InsertResult;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.language.ObjectIdFactory;
import com.torodb.mongodb.utils.DefaultIdUtils;
import com.torodb.mongowp.ErrorCode;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.bson.BsonObjectId;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.Request;

import java.util.stream.Stream;

import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class InsertImplementation
    implements WriteTransactionCommandImpl<InsertArgument, InsertResult> {

  @Override
  public Status<InsertResult> apply(Request req,
      Command<? super InsertArgument, ? super InsertResult> command, InsertArgument arg,
      WriteMongodTransaction context) {
    context.getMetrics().getInserts().mark(arg.getDocuments().size());

    ObjectIdFactory objectIdFactory = context.getObjectIdFactory();

    Stream<KvDocument> docsToInsert = arg.getDocuments().stream()
        .map(FromBsonValueTranslator.getInstance())
        .map((doc) -> (KvDocument) doc)
        .map(doc -> addId(objectIdFactory, doc));

    try {
      context.getDocTransaction().insert(
          req.getDatabase(),
          arg.getCollection(),
          docsToInsert
      );
    } catch (UserException ex) {
      //TODO: Improve error reporting
      return Status.from(ErrorCode.COMMAND_FAILED, ex.getLocalizedMessage());
    }

    return Status.ok(new InsertResult(arg.getDocuments().size()));
  }

  private KvDocument addId(ObjectIdFactory objectIdFactory, KvDocument doc) {
    if (doc.containsKey(DefaultIdUtils.ID_KEY)) {
      return doc;
    } else {
      BsonObjectId newId = objectIdFactory.consumeObjectId();
      return new KvDocument.Builder(doc)
          .putValue(DefaultIdUtils.ID_KEY, MongoWpConverter.translate(newId))
          .build();
    }
  }
}
