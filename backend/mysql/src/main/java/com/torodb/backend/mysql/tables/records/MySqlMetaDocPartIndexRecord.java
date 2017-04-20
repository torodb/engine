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

package com.torodb.backend.mysql.tables.records;

import com.torodb.backend.converters.TableRefConverter;
import com.torodb.backend.mysql.tables.MySqlMetaDocPartIndexTable;
import com.torodb.backend.tables.records.MetaDocPartIndexRecord;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonReader;

public class MySqlMetaDocPartIndexRecord extends MetaDocPartIndexRecord<String> {

  private static final long serialVersionUID = 2263619273193694206L;

  /**
   * Create a detached MetaIndexRecord
   */
  public MySqlMetaDocPartIndexRecord() {
    super(MySqlMetaDocPartIndexTable.DOC_PART_INDEX);
  }

  @Override
  public MySqlMetaDocPartIndexRecord values(String database, String identifier,
      String collection, String tableRef, Boolean unique) {
    setDatabase(database);
    setIdentifier(identifier);
    setCollection(collection);
    setTableRef(tableRef);
    setUnique(unique);
    return this;
  }

  /**
   * Create a detached, initialised MetaIndexRecord
   */
  public MySqlMetaDocPartIndexRecord(String database, String identifier, String collection,
      String tableRef, Boolean unique) {
    super(MySqlMetaDocPartIndexTable.DOC_PART_INDEX);

    values(database, identifier, collection, tableRef, unique);
  }

  @Override
  protected String toTableRefType(TableRef tableRef) {
    return TableRefConverter.toJsonArray(tableRef).toString();
  }

  @Override
  public TableRef getTableRefValue(TableRefFactory tableRefFactory) {
    final JsonReader reader = Json.createReader(new StringReader(getTableRef()));
    return TableRefConverter.fromJsonArray(tableRefFactory, reader.readArray());
  }

}
