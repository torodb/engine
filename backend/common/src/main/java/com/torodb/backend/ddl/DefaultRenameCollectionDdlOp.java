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

package com.torodb.backend.ddl;

import com.torodb.backend.SqlInterface;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaIndexField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaDocPartIndex;
import com.torodb.core.transaction.metainf.MutableMetaIndex;
import org.jooq.DSLContext;
import org.jooq.lambda.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

/**
 *
 */
public class DefaultRenameCollectionDdlOp implements RenameCollectionDdlOp {

  private final SqlInterface sqlInterface;
  private final IdentifierFactory identifierFactory;
  private final ReservedIdGenerator ridGenerator;

  @Inject
  public DefaultRenameCollectionDdlOp(SqlInterface sqlInterface,
      IdentifierFactory identifierFactory,
      ReservedIdGenerator ridGenerator) {
    this.sqlInterface = sqlInterface;
    this.identifierFactory = identifierFactory;
    this.ridGenerator = ridGenerator;
  }

  @Override
  public void renameCollection(DSLContext dsl, MetaDatabase fromDb,
      MetaCollection fromCol, MutableMetaDatabase toDb, MutableMetaCollection toCol) 
      throws RollbackException {

    copyMetaCollection(dsl, fromDb, fromCol, toDb, toCol);
    sqlInterface.getStructureInterface().renameCollection(
        dsl,
        fromDb.getIdentifier(),
        fromCol,
        toDb.getIdentifier(),
        toCol
    );
    sqlInterface.getMetaDataWriteInterface().deleteMetaCollection(
        dsl, fromDb, fromCol);
  }

  private void copyMetaCollection(DSLContext dsl, MetaDatabase fromDb, MetaCollection fromCol,
      MutableMetaDatabase toDb, MutableMetaCollection toCol) {
    Iterator<? extends MetaIndex> fromMetaIndexIterator = fromCol.streamContainedMetaIndexes()
        .iterator();
    while (fromMetaIndexIterator.hasNext()) {
      MetaIndex fromMetaIndex = fromMetaIndexIterator.next();
      MutableMetaIndex toMetaIndex = toCol.addMetaIndex(fromMetaIndex.getName(), fromMetaIndex
          .isUnique());
      sqlInterface.getMetaDataWriteInterface()
          .addMetaIndex(dsl, toDb, toCol, toMetaIndex);
      copyIndexFields(dsl, fromMetaIndex, toDb, toCol, toMetaIndex);
    }

    Iterator<? extends MetaDocPart> fromMetaDocPartIterator = fromCol.streamContainedMetaDocParts()
        .iterator();
    while (fromMetaDocPartIterator.hasNext()) {
      MetaDocPart fromMetaDocPart = fromMetaDocPartIterator.next();
      MutableMetaDocPart toMetaDocPart = toCol.addMetaDocPart(fromMetaDocPart.getTableRef(),
          identifierFactory.toDocPartIdentifier(
              toDb, toCol.getName(), fromMetaDocPart.getTableRef()));
      sqlInterface.getMetaDataWriteInterface().addMetaDocPart(
          dsl,
          toDb,
          toCol,
          toMetaDocPart
      );
      copyScalar(dsl, fromMetaDocPart, toDb, toCol, toMetaDocPart);
      copyFields(dsl, fromMetaDocPart, toDb, toCol, toMetaDocPart);
      copyIndexes(dsl, fromMetaDocPart, toDb, toCol, toMetaDocPart);
      int nextRid = ridGenerator.getDocPartRidGenerator(fromDb.getName(),fromCol.getName())
          .nextRid(fromMetaDocPart.getTableRef());
      ridGenerator.getDocPartRidGenerator(toDb.getName(), toCol.getName())
          .setNextRid(toMetaDocPart.getTableRef(), nextRid - 1);
    }
  }

  private void copyIndexFields(DSLContext dsl, MetaIndex fromMetaIndex,
      MetaDatabase toMetaDb, MetaCollection toMetaColl, MutableMetaIndex toMetaIndex) {
    Iterator<? extends MetaIndexField> fromMetaIndexFieldIterator = fromMetaIndex.iteratorFields();
    while (fromMetaIndexFieldIterator.hasNext()) {
      MetaIndexField fromMetaIndexField = fromMetaIndexFieldIterator.next();
      MetaIndexField toMetaIndexField = toMetaIndex.addMetaIndexField(
          fromMetaIndexField.getTableRef(),
          fromMetaIndexField.getFieldName(),
          fromMetaIndexField.getOrdering());
      sqlInterface.getMetaDataWriteInterface().addMetaIndexField(
          dsl, toMetaDb, toMetaColl, toMetaIndex, toMetaIndexField);
    }
  }

  private void copyScalar(DSLContext dsl, MetaDocPart fromMetaDocPart, MetaDatabase toMetaDb,
      MetaCollection toMetaCol, MutableMetaDocPart toMetaDocPart) {
    Iterator<? extends MetaScalar> fromMetaScalarIterator = fromMetaDocPart.streamScalars()
        .iterator();
    while (fromMetaScalarIterator.hasNext()) {
      MetaScalar fromMetaScalar = fromMetaScalarIterator.next();
      MetaScalar toMetaScalar = toMetaDocPart.addMetaScalar(
          identifierFactory.toFieldIdentifierForScalar(fromMetaScalar.getType()),
          fromMetaScalar.getType());
      sqlInterface.getMetaDataWriteInterface().addMetaScalar(
          dsl, toMetaDb, toMetaCol, toMetaDocPart, toMetaScalar);
    }
  }

  private void copyFields(DSLContext dsl, MetaDocPart fromMetaDocPart, MetaDatabase toMetaDb,
      MetaCollection toMetaColl, MutableMetaDocPart toMetaDocPart) {
    Iterator<? extends MetaField> fromMetaFieldIterator = fromMetaDocPart.streamFields().iterator();
    while (fromMetaFieldIterator.hasNext()) {
      MetaField fromMetaField = fromMetaFieldIterator.next();
      MetaField toMetaField = toMetaDocPart.addMetaField(
          fromMetaField.getName(),
          identifierFactory.toFieldIdentifier(
              toMetaDocPart,
              fromMetaField.getName(),
              fromMetaField.getType()
          ),
          fromMetaField.getType());
      sqlInterface.getMetaDataWriteInterface().addMetaField(
          dsl, toMetaDb, toMetaColl, toMetaDocPart, toMetaField);
    }
  }

  private void copyIndexes(DSLContext dsl, MetaDocPart fromMetaDocPart, MetaDatabase toMetaDb,
      MetaCollection toMetaColl, MutableMetaDocPart toMetaDocPart) {
    Iterator<? extends MetaIdentifiedDocPartIndex> fromMetaDocPartIndexIterator = fromMetaDocPart
        .streamIndexes().iterator();
    while (fromMetaDocPartIndexIterator.hasNext()) {
      MetaIdentifiedDocPartIndex fromMetaDocPartIndex = fromMetaDocPartIndexIterator.next();
      MutableMetaDocPartIndex toMutableMetaDocPartIndex = toMetaDocPart.addMetaDocPartIndex(
          fromMetaDocPartIndex.isUnique());
      List<Tuple2<String, Boolean>> identifiers =
          copyMetaIndexColumns(fromMetaDocPartIndex, toMutableMetaDocPartIndex);
      MetaIdentifiedDocPartIndex toMetaDocPartIndex = toMutableMetaDocPartIndex.immutableCopy(
          identifierFactory.toIndexIdentifier(
              toMetaDb,
              toMetaDocPart.getIdentifier(),
              identifiers)
      );
      sqlInterface.getMetaDataWriteInterface().addMetaDocPartIndex(
          dsl, toMetaDb, toMetaColl, toMetaDocPart, toMetaDocPartIndex);
      writeIndexColumns(dsl, toMetaDb, toMetaColl, toMetaDocPart, toMetaDocPartIndex);
    }
  }

  private List<Tuple2<String, Boolean>> copyMetaIndexColumns(
      MetaIdentifiedDocPartIndex fromMetaDocPartIndex,
      MutableMetaDocPartIndex toMetaDocPartIndex) {
    List<Tuple2<String, Boolean>> identifiers = new ArrayList<>();
    Iterator<? extends MetaDocPartIndexColumn> fromMetaDocPartIndexColumnIterator =
        fromMetaDocPartIndex.iteratorColumns();
    while (fromMetaDocPartIndexColumnIterator.hasNext()) {
      MetaDocPartIndexColumn fromMetaDocPartIndexColumn = fromMetaDocPartIndexColumnIterator.next();
      toMetaDocPartIndex.addMetaDocPartIndexColumn(
          fromMetaDocPartIndexColumn.getIdentifier(), fromMetaDocPartIndexColumn.getOrdering());
      identifiers.add(new Tuple2<>(fromMetaDocPartIndexColumn.getIdentifier(),
          fromMetaDocPartIndexColumn.getOrdering().isAscending()));
    }
    return identifiers;
  }

  private void writeIndexColumns(DSLContext dsl, MetaDatabase toMetaDb, MetaCollection toMetaCol,
      MetaDocPart toMetaDocPart,
      MetaIdentifiedDocPartIndex toMetaDocPartIndex) {
    Iterator<? extends MetaDocPartIndexColumn> toMetaDocPartIndexColumnIterator = toMetaDocPartIndex
        .iteratorColumns();
    while (toMetaDocPartIndexColumnIterator.hasNext()) {
      MetaDocPartIndexColumn toMetaDocPartIndexColumn = toMetaDocPartIndexColumnIterator.next();
      sqlInterface.getMetaDataWriteInterface().addMetaDocPartIndexColumn(
          dsl, toMetaDb, toMetaCol, toMetaDocPart, toMetaDocPartIndex,
          toMetaDocPartIndexColumn);
    }
  }

  @Override
  public void close() {}

}