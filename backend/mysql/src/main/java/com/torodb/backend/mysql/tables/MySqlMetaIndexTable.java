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

import com.torodb.backend.mysql.tables.records.MySqlMetaIndexRecord;
import com.torodb.backend.tables.MetaIndexTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class MySqlMetaIndexTable extends MetaIndexTable<MySqlMetaIndexRecord> {

  private static final long serialVersionUID = -6090026713335495681L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final MySqlMetaIndexTable INDEX = new MySqlMetaIndexTable();

  @Override
  public Class<MySqlMetaIndexRecord> getRecordType() {
    return MySqlMetaIndexRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public MySqlMetaIndexTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public MySqlMetaIndexTable(String alias) {
    this(alias, MySqlMetaIndexTable.INDEX);
  }

  private MySqlMetaIndexTable(String alias, Table<MySqlMetaIndexRecord> aliased) {
    this(alias, aliased, null);
  }

  private MySqlMetaIndexTable(String alias, Table<MySqlMetaIndexRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MySqlMetaIndexTable as(String alias) {
    return new MySqlMetaIndexTable(alias, this);
  }

  /**
   * Rename this table
   */
  public MySqlMetaIndexTable rename(String name) {
    return new MySqlMetaIndexTable(name, null);
  }

  @Override
  protected TableField<MySqlMetaIndexRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<MySqlMetaIndexRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), 
        this, "");
  }

  @Override
  protected TableField<MySqlMetaIndexRecord, String> createNameField() {
    return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlMetaIndexRecord, Boolean> createUniqueField() {
    return createField(TableFields.UNIQUE.fieldName, SQLDataType.BOOLEAN.nullable(false), this, 
        "");
  }

}
