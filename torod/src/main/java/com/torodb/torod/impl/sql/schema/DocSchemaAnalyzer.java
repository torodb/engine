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

package com.torodb.torod.impl.sql.schema;

import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.kvdocument.values.KvDocument;

import java.util.stream.Stream;

import javax.inject.Inject;

/**
 * Analyze a batch of documents to look for any required change on the schema.
 * 
 * <p/>NOTE: for historically reasons, transformation to relational model and changes on the 
 * metamodel cache were done at the same time. Since we are using a independent thread schema
 * manager, it has no sense to do it in that way, but algorithms haven't been changed yet, so the
 * user thread transforms the data and then, if it detects a change on the metamodel, delegates on
 * the schema manager and it will execute the same code again (translating the data a second time)
 * and once the schema is changed, the user thread will transform the data again. Therefore, if
 * there a batch requires changes on the metamodel, it is translated three times!
 */
public class DocSchemaAnalyzer {

  private final D2RTranslatorFactory translatorFactory;

  @Inject
  public DocSchemaAnalyzer(D2RTranslatorFactory translatorFactory) {
    this.translatorFactory = translatorFactory;
  }

  /**
   * Analyze the given documents, modifying the collection so it is ensured that all documents
   * <em>fit</em> in the meta collection.
   *
   * <p>A document <em>fits</em> in a collection if it can be stored there without schema
   * modifications, so all doc parts, scalar and fields required to store the document are declared
   * on the collection.
   *
   * @param db   The database on which documents will be inserted
   * @param col  The collection that will be modified so it can store the given documents. Append
   *             only operations can be executed on this collection.
   * @param docs The documents that will be inserted
   */
  public void analyze(MetaDatabase db, MutableMetaCollection col, Stream<KvDocument> docs) {
    D2RTranslator translator = translatorFactory.createTranslator(db, col);

    docs.forEach(translator::translate);
  }

}