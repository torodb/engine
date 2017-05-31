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

package com.torodb.backend.mysql.tables;

import com.torodb.backend.mysql.tables.records.MySqlKvRecord;
import com.torodb.backend.tables.KvTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class MySqlKvTable extends KvTable<MySqlKvRecord> {

  private static final long serialVersionUID = -5506554761865128847L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final MySqlKvTable KV = new MySqlKvTable();

  @Override
  public Class<MySqlKvRecord> getRecordType() {
    return MySqlKvRecord.class;
  }

  public MySqlKvTable() {
    this(TABLE_NAME, null);
  }

  public MySqlKvTable(String alias) {
    this(alias, MySqlKvTable.KV);
  }

  private MySqlKvTable(String alias, Table<MySqlKvRecord> aliased) {
    this(alias, aliased, null);
  }

  private MySqlKvTable(
      String alias,
      Table<MySqlKvRecord> aliased, Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MySqlKvTable as(String alias) {
    return new MySqlKvTable(alias, this);
  }

  /**
   * Rename this table
   */
  public MySqlKvTable rename(String name) {
    return new MySqlKvTable(name, null);
  }

  @Override
  protected TableField<MySqlKvRecord, String> createNameField() {
    return createField(TableFields.KEY.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlKvRecord, String> createIdentifierField() {
    return createField(TableFields.VALUE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }
}
