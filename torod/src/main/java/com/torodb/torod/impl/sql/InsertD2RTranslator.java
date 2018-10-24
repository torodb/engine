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

package com.torodb.torod.impl.sql;

import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.WrapperMutableMetaCollection;
import com.torodb.kvdocument.values.KvDocument;

import java.util.stream.Stream;

import javax.inject.Inject;

/**
 * Translates a batch of documents into a relational representation.
 *
 * <p/>NOTE: for historically reasons, transformation to relational model and changes on the
 * metamodel cache were done at the same time. Since we are using a independent thread schema
 * manager, it has no sense to do it in that way, but algorithms haven't been changed yet, so the
 * user thread transforms the data and then, if it detects a change on the metamodel, delegates on
 * the schema manager and it will execute the same code again (translating the data a second time)
 * and once the schema is changed, the user thread will transform the data again. Therefore, if
 * there a batch requires changes on the metamodel, it is translated three times!
 */
class InsertD2RTranslator {

  private final D2RTranslatorFactory translatorFactory;

  @Inject
  public InsertD2RTranslator(D2RTranslatorFactory translatorFactory) {
    this.translatorFactory = translatorFactory;
  }

  /**
   * Translate a batch of documents into a {@link CollectionData} if they <em>fit</em> in the given
   * collection.
   *
   * <p>A document <em>fits</em> in a collection if it can be stored there without schema
   * modifications, so all doc parts, scalar and fields required to store the document are declared
   * on the collection. If there is at least one document that doesn't fit in the given collection,
   * a {@link IncompatibleSchemaException} is thrown.
   *
   * @param snapshot The current view of the metadata
   * @param dbName   The name of the database on which documents would be inserted
   * @param colName  The name of the collection on which documents would be inserteds.
   * @param docs     The documents that will be inserted
   * @return A {@link CollectionData} with the same documents translated to a relational
   *         representation.
   * @throws IncompatibleSchemaException if there is no database with that name or if the database
   *                                     does not contains a collection with that name or there is
   *                                     at least one document that doesn't fit on the given
   *                                     collection
   */
  CollectionData analyze(ImmutableMetaSnapshot snapshot, String dbName, String colName,
      Stream<KvDocument> docs) throws IncompatibleSchemaException {

    ImmutableMetaDatabase db = snapshot.getMetaDatabaseByName(dbName);
    if (db == null) {
      throw new IncompatibleSchemaException();
    }
    ImmutableMetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      throw new IncompatibleSchemaException();
    }
    
    MutableMetaCollection mutableCol = new WrapperMutableMetaCollection(col);

    D2RTranslator translator = translatorFactory.createTranslator(db, mutableCol);

    docs.forEach(translator::translate);

    if (mutableCol.hasChanges()) {
      throw new IncompatibleSchemaException();
    }

    return translator.getCollectionDataAccumulator();
  }


}