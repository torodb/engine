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

package com.torodb.mongodb.commands.impl.admin;

import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.mongodb.commands.impl.RetrierSchemaCommandImpl;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.pojos.index.IndexOptions.KnownType;
import com.torodb.mongodb.commands.pojos.index.type.AscIndexType;
import com.torodb.mongodb.commands.pojos.index.type.DefaultIndexTypeVisitor;
import com.torodb.mongodb.commands.pojos.index.type.DescIndexType;
import com.torodb.mongodb.commands.pojos.index.type.IndexType;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand.CreateIndexesResult;
import com.torodb.mongowp.ErrorCode;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.Request;
import com.torodb.mongowp.exceptions.CommandFailed;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.SchemaOperationExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class CreateIndexesImplementation extends
    RetrierSchemaCommandImpl<CreateIndexesArgument, CreateIndexesResult> {

  @SuppressWarnings("checkstyle:LineLength")
  private static final FieldIndexOrderingConverterIndexTypeVisitor fieldIndexOrderingConverterVisitor =
      new FieldIndexOrderingConverterIndexTypeVisitor();

  public CreateIndexesImplementation(LoggerFactory loggerFactory) {
    super(loggerFactory);
  }


  @Override
  public Status<CreateIndexesResult> tryApply(Request req,
      Command<? super CreateIndexesArgument, ? super CreateIndexesResult> command,
      CreateIndexesArgument arg, SchemaOperationExecutor context) {
    int indexesBefore = (int) context.getIndexesInfo(req.getDatabase(), arg.getCollection())
        .count();
    int indexesAfter = indexesBefore;

    try {
      boolean createdCollectionAutomatically = context.prepareSchema(
          req.getDatabase(),
          arg.getCollection(),
          Collections.emptyList()
      );

      for (IndexOptions indexOptions : arg.getIndexesToCreate()) {
        if (indexOptions.getKeys().size() < 1) {
          return Status.from(ErrorCode.CANNOT_CREATE_INDEX, "Index keys cannot be empty.");
        }

        if (indexOptions.isBackground()) {
          throw new CommandFailed("createIndexes",
              "Building index in background is not supported right now");
        }

        if (indexOptions.isSparse()) {
          throw new CommandFailed("createIndexes",
              "Sparse index are not supported right now");
        }

        List<IndexFieldInfo> fields = new ArrayList<>(indexOptions.getKeys().size());
        for (IndexOptions.Key indexKey : indexOptions.getKeys()) {
          AttributeReference.Builder attRefBuilder = new AttributeReference.Builder();
          for (String key : indexKey.getKeys()) {
            attRefBuilder.addObjectKey(key);
          }

          IndexType indexType = indexKey.getType();

          if (!KnownType.contains(indexType)) {
            return Status.from(ErrorCode.CANNOT_CREATE_INDEX,
                "bad index key pattern: Unknown index plugin '"
                + indexKey.getType().getName() + "'");
          }

          Optional<FieldIndexOrdering> ordering = indexType.accept(
              fieldIndexOrderingConverterVisitor, null);
          if (!ordering.isPresent()) {
            throw new CommandFailed("createIndexes",
                "Index of type " + indexType.getName() + " is not supported right now");
          }

          fields.add(new IndexFieldInfo(attRefBuilder.build(), ordering.get().isAscending()));
        }

        if (context.createIndex(req.getDatabase(), arg.getCollection(),
            indexOptions.getName(), fields, indexOptions.isUnique())) {
          indexesAfter++;
        }
      }

      String note = null;

      if (indexesAfter == indexesBefore) {
        note = "all indexes already exist";
      }

      return Status.ok(new CreateIndexesResult(indexesBefore, indexesAfter, note,
          createdCollectionAutomatically));
    } catch (UserException ex) {
      return Status.from(ErrorCode.COMMAND_FAILED, ex.getLocalizedMessage());
    } catch (CommandFailed ex) {
      return Status.from(ex);
    }
  }

  private static class FieldIndexOrderingConverterIndexTypeVisitor
      extends DefaultIndexTypeVisitor<Void, Optional<FieldIndexOrdering>> {

    @Override
    protected Optional<FieldIndexOrdering> defaultVisit(IndexType indexType, Void arg) {
      return Optional.empty();
    }

    @Override
    public Optional<FieldIndexOrdering> visit(AscIndexType indexType, Void arg) {
      return Optional.of(FieldIndexOrdering.ASC);
    }

    @Override
    public Optional<FieldIndexOrdering> visit(DescIndexType indexType, Void arg) {
      return Optional.of(FieldIndexOrdering.DESC);
    }
  }
}
