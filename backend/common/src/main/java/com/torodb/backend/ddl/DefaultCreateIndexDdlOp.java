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

import com.google.common.base.Preconditions;
import com.torodb.backend.BackendLoggerFactory;
import com.torodb.backend.SqlInterface;
import com.torodb.core.TableRef;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaIndexField;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaDocPartIndex;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.lambda.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

/**
 *
 */
public class DefaultCreateIndexDdlOp implements CreateIndexDdlOp {

  private static final Logger LOGGER = BackendLoggerFactory.get(DefaultCreateIndexDdlOp.class);

  private final SqlInterface sqlInterface;
  private final IdentifierFactory identifierFactory;

  @Inject
  public DefaultCreateIndexDdlOp(SqlInterface sqlInterface, IdentifierFactory identifierFactory) {
    this.sqlInterface = sqlInterface;
    this.identifierFactory = identifierFactory;
  }

  @Override
  public void createIndex(DSLContext dsl, MetaDatabase db,
      MutableMetaCollection col, MetaIndex index) throws UserException {

    Preconditions.checkArgument(!index.isUnique() || index.streamTableRefs().count() == 1,
        "composed unique indexes on fields of different subdocuments are not supported yet");

    sqlInterface.getMetaDataWriteInterface().addMetaIndex(
        dsl, db, col, index
    );

    Iterator<? extends MetaIndexField> indexFieldIterator = index.iteratorFields();
    while (indexFieldIterator.hasNext()) {
      MetaIndexField field = indexFieldIterator.next();
      sqlInterface.getMetaDataWriteInterface().addMetaIndexField(
          dsl, db, col, index, field
      );
    }

    createMissingDocPartIndexes(dsl, db, col, index);
  }

  private void createIndex(DSLContext dsl, MetaDatabase db, MetaCollection col, MetaIndex index,
      MutableMetaDocPart docPart,
      List<String> identifiers) throws UserException {
    MutableMetaDocPartIndex docPartIndex = docPart.addMetaDocPartIndex(index.isUnique());
    Iterator<? extends MetaIndexField> indexFieldIterator = index.iteratorMetaIndexFieldByTableRef(
        docPart.getTableRef());
    int position = 0;
    List<Tuple3<String, Boolean, FieldType>> columnList = new ArrayList<>(identifiers.size());
    for (String identifier : identifiers) {
      MetaIndexField indexField = indexFieldIterator.next();
      MetaDocPartIndexColumn docPartIndexColumn = docPartIndex.putMetaDocPartIndexColumn(
          position++,
          identifier, 
          indexField.getOrdering()
      );
      columnList.add(new Tuple3<>(docPartIndexColumn.getIdentifier(), docPartIndexColumn
          .getOrdering().isAscending(), docPart.getMetaFieldByIdentifier(identifier).getType()));
    }
    MetaIdentifiedDocPartIndex identifiedDocPartIndex = docPartIndex.immutableCopy(
        identifierFactory.toIndexIdentifier(
            db, docPart.getIdentifier(), columnList
        )
    );

    sqlInterface.getMetaDataWriteInterface().addMetaDocPartIndex(
        dsl, db, col, docPart, identifiedDocPartIndex);

    for (String identifier : identifiers) {
      MetaDocPartIndexColumn docPartIndexColumn = docPartIndex
          .getMetaDocPartIndexColumnByIdentifier(identifier);
      sqlInterface.getMetaDataWriteInterface().addMetaDocPartIndexColumn(
          dsl, db, col, docPart, identifiedDocPartIndex, docPartIndexColumn);
    }

    sqlInterface.getStructureInterface().createIndex(
        dsl, identifiedDocPartIndex.getIdentifier(), db.getIdentifier(), docPart
        .getIdentifier(),
        columnList, index.isUnique());
    LOGGER.info("Created index {} for table {} associated to logical index {}.{}.{}",
        identifiedDocPartIndex.getIdentifier(), docPart.getIdentifier(), db.getName(),
        col.getName(), index.getName());
  }

  private void createMissingDocPartIndexes(DSLContext dsl, MetaDatabase db, 
      MutableMetaCollection col, MetaIndex index) throws UserException {
    Iterator<TableRef> tableRefIterator = index.streamTableRefs().iterator();
    while (tableRefIterator.hasNext()) {
      TableRef tableRef = tableRefIterator.next();
      MutableMetaDocPart docPart = col.getMetaDocPartByTableRef(tableRef);
      if (docPart != null && index.isCompatible(docPart)) {
        Iterator<List<String>> docPartIndexesFieldsIterator =
            index.iteratorMetaDocPartIndexesIdentifiers(docPart);

        while (docPartIndexesFieldsIterator.hasNext()) {
          List<String> identifiers = docPartIndexesFieldsIterator.next();
          boolean containsExactDocPartIndex = docPart.streamIndexes()
              .anyMatch(docPartIndex -> index.isMatch(docPart, identifiers, docPartIndex));
          if (!containsExactDocPartIndex) {
            createIndex(dsl, db, col, index, docPart, identifiers);
          }
        }
      }
    }
  }

  @Override
  public void close() {}
}