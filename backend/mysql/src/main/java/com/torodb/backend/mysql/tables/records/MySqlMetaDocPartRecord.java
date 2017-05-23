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
import com.torodb.backend.mysql.tables.MySqlMetaDocPartTable;
import com.torodb.backend.tables.records.MetaDocPartRecord;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonReader;

public class MySqlMetaDocPartRecord extends MetaDocPartRecord<String> {

  private static final long serialVersionUID = 4525720333148409410L;

  /**
   * Create a detached MetaDocPartRecord
   */
  public MySqlMetaDocPartRecord() {
    super(MySqlMetaDocPartTable.DOC_PART);
  }

  /**
   * Create a detached, initialised MetaDocPartRecord
   */
  public MySqlMetaDocPartRecord(String database, String collection, String tableRef,
      String identifier, Integer lastRid) {
    super(MySqlMetaDocPartTable.DOC_PART);

    values(database, collection, tableRef, identifier, lastRid);
  }

  @Override
  public MySqlMetaDocPartRecord values(String database, String collection, String tableRef,
      String identifier, Integer lastRid) {
    setDatabase(database);
    setCollection(collection);
    setTableRef(tableRef);
    setIdentifier(identifier);
    setLastRid(lastRid);
    return this;
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
