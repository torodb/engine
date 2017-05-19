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

package com.torodb.mongodb.repl.commands.impl;

import com.torodb.core.exceptions.user.UnsupportedCompoundIndexException;
import com.torodb.core.exceptions.user.UnsupportedUniqueIndexException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.pojos.index.IndexOptions.KnownType;
import com.torodb.mongodb.commands.pojos.index.type.AscIndexType;
import com.torodb.mongodb.commands.pojos.index.type.DefaultIndexTypeVisitor;
import com.torodb.mongodb.commands.pojos.index.type.DescIndexType;
import com.torodb.mongodb.commands.pojos.index.type.IndexType;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand.CreateIndexesResult;
import com.torodb.mongodb.filters.IndexFilter;
import com.torodb.mongowp.ErrorCode;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.Request;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.SchemaOperationExecutor;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;

public class CreateIndexesReplImpl
    extends ReplCommandImpl<CreateIndexesArgument, CreateIndexesResult> {

  @SuppressWarnings("checkstyle:LineLength")
  private static final FieldIndexOrderingConverterIndexTypeVisitor fieldIndexOrderingConverterVisitor =
      new FieldIndexOrderingConverterIndexTypeVisitor();
  private final Logger logger;
  private final CommandFilterUtil filterUtil;
  private final IndexFilter indexFilter;

  @Inject
  public CreateIndexesReplImpl(CommandFilterUtil filterUtil, IndexFilter indexFilter,
      LoggerFactory lf) {
    this.logger = lf.apply(this.getClass());
    this.filterUtil = filterUtil;
    this.indexFilter = indexFilter;
  }

  @Override
  public Status<CreateIndexesResult> apply(Request req,
      Command<? super CreateIndexesArgument, ? super CreateIndexesResult> command,
      CreateIndexesArgument arg, SchemaOperationExecutor schemaEx) {

    if (!filterUtil.testNamespaceFilter(req.getDatabase(), arg.getCollection(), command)) {
      return Status.ok(new CreateIndexesResult(0, 0, null, false));
    }

    int indexesBefore = (int) schemaEx.getIndexesInfo(req.getDatabase(), arg.getCollection())
        .count();
    int indexesAfter = indexesBefore;

    try {
      boolean createdCollectionAutomatically = schemaEx.prepareSchema(
          req.getDatabase(),
          arg.getCollection(),
          Collections.emptyList()
      );

      for (IndexOptions indexOptions : arg.getIndexesToCreate()) {
        assert req.getDatabase().equals(indexOptions.getDatabase()) : "Database modified by the "
            + "request (" + req.getDatabase() + ") is different than the one specified on index "
            + indexOptions.getName();
        assert arg.getCollection().equals(indexOptions.getCollection()) : "Collection modified by "
            + "the request (" + arg.getCollection() + ") is different than the one specified on "
            + "index " + indexOptions.getName();
        if (!indexFilter.filter(indexOptions)) {
          logger.info("Skipping filtered index {}.{}.{}.",
              indexOptions.getDatabase(), indexOptions.getCollection(), indexOptions.getName());
          continue;
        }

        if (indexOptions.getKeys().size() < 1) {
          return Status.from(ErrorCode.CANNOT_CREATE_INDEX, "Index keys cannot be empty.");
        }

        if (indexOptions.isBackground()) {
          logger.info("Building index in background is not supported. Ignoring option");
        }

        if (indexOptions.isSparse()) {
          logger.info("Sparse index are not supported. Ignoring option");
        }

        boolean skipIndex = false;
        List<IndexFieldInfo> fields = new ArrayList<>(indexOptions.getKeys().size());
        for (IndexOptions.Key indexKey : indexOptions.getKeys()) {
          AttributeReference.Builder attRefBuilder = new AttributeReference.Builder();
          for (String key : indexKey.getKeys()) {
            attRefBuilder.addObjectKey(key);
          }

          IndexType indexType = indexKey.getType();

          if (!KnownType.contains(indexType)) {
            String note = "Bad index key pattern: Unknown index type '"
                + indexKey.getType().getName() + "'. Skipping index.";
            logger.info(note);
            skipIndex = true;
            break;
          }

          Optional<FieldIndexOrdering> ordering = indexType.accept(
              fieldIndexOrderingConverterVisitor, null);
          if (!ordering.isPresent()) {
            String note = "Index of type " + indexType.getName()
                + " is not supported. Skipping index.";
            logger.info(note);
            skipIndex = true;
            break;
          }

          fields.add(new IndexFieldInfo(attRefBuilder.build(), ordering.get().isAscending()));
        }

        if (skipIndex) {
          continue;
        }

        try {
          logger.info("Creating index {} on collection {}.{}",
              indexOptions.getName(), req.getDatabase(), arg.getCollection());

          if (schemaEx.createIndex(req.getDatabase(), arg.getCollection(), indexOptions.getName(),
              fields, indexOptions.isUnique())) {
            indexesAfter++;
          }
        } catch (UnsupportedCompoundIndexException ex) {
          String note =
              "Compound indexes are not supported. Skipping index.";
          logger.info(note);
          continue;
        } catch (UnsupportedUniqueIndexException ex) {
          String note =
              "Unique index with keys on distinct subdocuments is not supported. Skipping index.";
          logger.info(note);
          continue;
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
