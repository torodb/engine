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

import com.torodb.core.language.AttributeReference;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.mongodb.commands.impl.RetrierSchemaCommandImpl;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.pojos.index.IndexOptions.KnownType;
import com.torodb.mongodb.commands.signatures.admin.DropIndexesCommand.DropIndexesArgument;
import com.torodb.mongodb.commands.signatures.admin.DropIndexesCommand.DropIndexesResult;
import com.torodb.mongodb.utils.DefaultIdUtils;
import com.torodb.mongowp.ErrorCode;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.commands.Command;
import com.torodb.mongowp.commands.Request;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.SchemaOperationExecutor;
import com.torodb.torod.exception.UnexistentCollectionException;
import com.torodb.torod.exception.UnexistentDatabaseException;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;


public class DropIndexesImplementation extends
    RetrierSchemaCommandImpl<DropIndexesArgument, DropIndexesResult> {

  public DropIndexesImplementation(LoggerFactory loggerFactory) {
    super(loggerFactory);
  }

  @Override
  public Status<DropIndexesResult> tryApply(Request req,
      Command<? super DropIndexesArgument, ? super DropIndexesResult> command,
      DropIndexesArgument arg, SchemaOperationExecutor context) {
    List<IndexInfo> indexesInfo = context.getIndexesInfo(req.getDatabase(), arg.getCollection())
        .collect(Collectors.toList());
    int indexesBefore = indexesInfo.size();

    List<String> indexesToDrop;

    if (!arg.isDropAllIndexes()) {
      if (!arg.isDropByKeys()) {
        if (DefaultIdUtils.ID_INDEX.equals(arg.getIndexToDrop())) {
          return Status.from(ErrorCode.INVALID_OPTIONS, "cannot drop _id index");
        }
        indexesToDrop = Arrays.asList(arg.getIndexToDrop());
      } else {
        assert arg.getKeys() != null;
        if (arg.getKeys().stream().anyMatch(key -> !(KnownType.contains(key.getType())) || (key
            .getType() != KnownType.asc.getIndexType() && key.getType() != KnownType.desc
            .getIndexType()))) {
          return getStatusForIndexNotFoundWithKeys(arg);
        }

        indexesToDrop = indexesInfo.stream()
            .filter(index -> indexFieldsMatchKeys(index, arg.getKeys()))
            .map(index -> index.getName())
            .collect(Collectors.toList());

        if (indexesToDrop.isEmpty()) {
          return getStatusForIndexNotFoundWithKeys(arg);
        }
      }
    } else {
      indexesToDrop = indexesInfo.stream()
          .filter(indexInfo -> !DefaultIdUtils.ID_INDEX.equals(indexInfo.getName()))
          .map(indexInfo -> indexInfo.getName())
          .collect(Collectors.toList());
    }

    for (String indexToDrop : indexesToDrop) {
      boolean dropped = false;
      try {
        dropped = context.dropIndex(
            req.getDatabase(),
            arg.getCollection(),
            indexToDrop
        );
      } catch (UnexistentCollectionException ex) {
        getLogger().debug("Trying to remove the index {} on the unexistent collection {}.{}",
            indexToDrop, req.getDatabase(), arg.getCollection());
      } catch (UnexistentDatabaseException ex) {
        getLogger().debug("Trying to remove the index {}.{}.{}, but that database doesn't exist",
            req.getDatabase(), arg.getCollection(), indexToDrop);
      }
      if (!dropped) {
        getLogger().info("Index {}.{}.{} not found",
            req.getDatabase(), arg.getCollection(), indexToDrop);
      }
    }

    return Status.ok(new DropIndexesResult(indexesBefore));
  }

  private Status<DropIndexesResult> getStatusForIndexNotFoundWithKeys(DropIndexesArgument arg) {
    return Status.from(ErrorCode.INDEX_NOT_FOUND, "index not found with keys [" + arg.getKeys()
        .stream()
        .map(key -> '"' + key.getKeys()
            .stream()
            .collect(Collectors.joining(".")) + "\" :" + key.getType().getName())
        .collect(Collectors.joining(", ")) + "]");
  }

  private boolean indexFieldsMatchKeys(IndexInfo index, List<IndexOptions.Key> keys) {
    if (index.getFields().size() != keys.size()) {
      return false;
    }

    Iterator<IndexFieldInfo> fieldsIterator = index.getFields().iterator();
    Iterator<IndexOptions.Key> keysIterator = keys.iterator();
    while (fieldsIterator.hasNext()) {
      IndexFieldInfo field = fieldsIterator.next();
      IndexOptions.Key key = keysIterator.next();

      if ((field.isAscending() && key.getType() != KnownType.asc.getIndexType()) || (!field
          .isAscending() && key.getType() != KnownType.desc.getIndexType()) || (field
          .getAttributeReference().getKeys().size() != key.getKeys().size())) {
        return false;
      }

      Iterator<AttributeReference.Key<?>> fieldKeysIterator = field.getAttributeReference()
          .getKeys().iterator();
      Iterator<String> keyKeysIterator = key.getKeys().iterator();

      while (fieldKeysIterator.hasNext()) {
        AttributeReference.Key<?> fieldKey = fieldKeysIterator.next();
        String keyKey = keyKeysIterator.next();

        if (!fieldKey.toString().equals(keyKey)) {
          return false;
        }
      }
    }

    return true;
  }

}
