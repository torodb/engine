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

import com.torodb.backend.BackendLoggerFactory;
import com.torodb.backend.SqlInterface;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import java.util.Iterator;

import javax.inject.Inject;

/**
 *
 */
public class DefaultDropIndexDdlOp implements DropIndexDdlOp {

  private static final Logger LOGGER = BackendLoggerFactory.get(DefaultDropIndexDdlOp.class);

  private final SqlInterface sqlInterface;

  @Inject
  public DefaultDropIndexDdlOp(SqlInterface sqlInterface) {
    this.sqlInterface = sqlInterface;
  }

  @Override
  public void dropIndex(DSLContext dsl, MetaDatabase db,
      MutableMetaCollection col, MetaIndex index) {

    sqlInterface.getMetaDataWriteInterface().deleteMetaIndex(
        dsl, db, col, index);
    Iterator<TableRef> tableRefIterator = index.streamTableRefs().iterator();
    while (tableRefIterator.hasNext()) {
      TableRef tableRef = tableRefIterator.next();
      MutableMetaDocPart docPart = col.getMetaDocPartByTableRef(tableRef);
      if (docPart != null) {
        Iterator<? extends MetaIdentifiedDocPartIndex> docPartIndexesIterator =
            docPart.streamIndexes().iterator();

        while (docPartIndexesIterator.hasNext()) {
          MetaIdentifiedDocPartIndex docPartIndex = docPartIndexesIterator.next();
          if (index.isCompatible(docPart, docPartIndex)) {
            boolean existsAnyOtherCompatibleIndex = col.streamContainedMetaIndexes()
                .anyMatch(otherIndex -> otherIndex != index && otherIndex.isCompatible(docPart,
                    docPartIndex));
            if (!existsAnyOtherCompatibleIndex) {
              dropIndex(dsl, db, col, docPart, docPartIndex);
              LOGGER.info("Dropped index {} for table {}", docPartIndex.getIdentifier(), docPart
                  .getIdentifier());
            }
          }
        }
      }
    }
  }

  private void dropIndex(DSLContext dsl, MetaDatabase db, MetaCollection col,
      MutableMetaDocPart docPart, MetaIdentifiedDocPartIndex docPartIndex) {
    docPart.removeMetaDocPartIndexByIdentifier(docPartIndex.getIdentifier());

    sqlInterface.getMetaDataWriteInterface().deleteMetaDocPartIndex(
        dsl,
        db,
        col,
        docPart,
        docPartIndex
    );

    sqlInterface.getStructureInterface().dropIndex(
        dsl, db.getIdentifier(), docPart.getIdentifier(), docPartIndex.getIdentifier());
  }

  @Override
  public void close() {}

}