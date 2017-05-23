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

import com.torodb.backend.mysql.tables.records.MySqlMetaDatabaseRecord;
import com.torodb.backend.tables.MetaDatabaseTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class MySqlMetaDatabaseTable extends MetaDatabaseTable<MySqlMetaDatabaseRecord> {

  private static final long serialVersionUID = -5506554761865128847L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final MySqlMetaDatabaseTable DATABASE = new MySqlMetaDatabaseTable();

  @Override
  public Class<MySqlMetaDatabaseRecord> getRecordType() {
    return MySqlMetaDatabaseRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public MySqlMetaDatabaseTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public MySqlMetaDatabaseTable(String alias) {
    this(alias, MySqlMetaDatabaseTable.DATABASE);
  }

  private MySqlMetaDatabaseTable(String alias, Table<MySqlMetaDatabaseRecord> aliased) {
    this(alias, aliased, null);
  }

  private MySqlMetaDatabaseTable(String alias, Table<MySqlMetaDatabaseRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MySqlMetaDatabaseTable as(String alias) {
    return new MySqlMetaDatabaseTable(alias, this);
  }

  /**
   * Rename this table
   */
  public MySqlMetaDatabaseTable rename(String name) {
    return new MySqlMetaDatabaseTable(name, null);
  }

  @Override
  protected TableField<MySqlMetaDatabaseRecord, String> createNameField() {
    return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlMetaDatabaseRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), 
        this, "");
  }
}
