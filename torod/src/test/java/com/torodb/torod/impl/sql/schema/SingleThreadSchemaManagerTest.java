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

import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.MemoryRidGenerator;
import com.torodb.core.d2r.MockIdentifierFactory;
import com.torodb.core.d2r.impl.D2RTranslatorStack;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;


/**
 *
 */
@RunWith(JUnitPlatform.class)
public class SingleThreadSchemaManagerTest extends SchemaManagerContract {

  private final IdentifierFactory idFactory = new MockIdentifierFactory();
  private final DocSchemaAnalyzer docSchemaAnalyzer = new DocSchemaAnalyzer(
      (MetaDatabase db, MutableMetaCollection col) -> new D2RTranslatorStack(
          getTableRefFactory(),
          idFactory,
          new MemoryRidGenerator(),
          db,
          col)
  );

  @Override
  protected SchemaManager createManager() {
    
    Supervisor supervisor = (supervised, error) -> {
      throw new AssertionError("Object " + supervised + " thrown an exception", error);
    };

    Logic logic = new Logic(
        idFactory,
        getTableRefFactory()::translate,
        docSchemaAnalyzer,
        supervisor
    );

    return new SingleThreadSchemaManager(logic);
  }

  @Override
  protected DocSchemaAnalyzer getDocSchemaAnalyzer() {
    return docSchemaAnalyzer;
  }
}