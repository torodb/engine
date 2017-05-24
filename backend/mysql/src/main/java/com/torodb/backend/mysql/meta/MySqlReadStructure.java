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
package com.torodb.backend.mysql.meta;

import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.ddl.DefaultReadStructure;
import com.torodb.backend.meta.SchemaValidator;
import com.torodb.backend.tables.records.MetaDatabaseRecord;
import com.torodb.core.TableRefFactory;
import org.jooq.DSLContext;

import javax.inject.Inject;

public class MySqlReadStructure extends DefaultReadStructure {

  @Inject
  public MySqlReadStructure(SqlInterface sqlInterface, SqlHelper sqlHelper,
      TableRefFactory tableRefFactory) {
    super(sqlInterface, sqlHelper, tableRefFactory);
  }

  @Override
  protected DefaultReadStructure.Updater createUpdater(DSLContext dsl) {
    return new Updater(dsl, tableRefFactory, sqlInterface);
  }

  protected static class Updater extends DefaultReadStructure.Updater {

    public Updater(DSLContext dsl, TableRefFactory tableRefFactory, SqlInterface sqlInterface) {
      super(dsl, tableRefFactory, sqlInterface);
    }

    protected SchemaValidator createSchemaValidator(MetaDatabaseRecord databaseRecord) {
      return new MySqlSchemaValidator(dsl, databaseRecord.getIdentifier(),
          databaseRecord.getName());
    }
    
  }
  
}
