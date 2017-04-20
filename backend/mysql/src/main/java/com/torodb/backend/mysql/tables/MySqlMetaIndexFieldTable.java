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

import com.torodb.backend.converters.jooq.OrderingConverter;
import com.torodb.backend.mysql.tables.records.MySqlMetaIndexFieldRecord;
import com.torodb.backend.tables.MetaIndexFieldTable;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
@SuppressWarnings("checkstyle:LineLength")
public class MySqlMetaIndexFieldTable extends MetaIndexFieldTable<String, MySqlMetaIndexFieldRecord> {

  private static final long serialVersionUID = 8649935905000022435L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final MySqlMetaIndexFieldTable INDEX_FIELD =
      new MySqlMetaIndexFieldTable();

  @Override
  public Class<MySqlMetaIndexFieldRecord> getRecordType() {
    return MySqlMetaIndexFieldRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public MySqlMetaIndexFieldTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public MySqlMetaIndexFieldTable(String alias) {
    this(alias, MySqlMetaIndexFieldTable.INDEX_FIELD);
  }

  private MySqlMetaIndexFieldTable(String alias, Table<MySqlMetaIndexFieldRecord> aliased) {
    this(alias, aliased, null);
  }

  private MySqlMetaIndexFieldTable(String alias, Table<MySqlMetaIndexFieldRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MySqlMetaIndexFieldTable as(String alias) {
    return new MySqlMetaIndexFieldTable(alias, this);
  }

  /**
   * Rename this table
   */
  public MySqlMetaIndexFieldTable rename(String name) {
    return new MySqlMetaIndexFieldTable(name, null);
  }

  @Override
  protected TableField<MySqlMetaIndexFieldRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlMetaIndexFieldRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<MySqlMetaIndexFieldRecord, String> createIndexField() {
    return createField(TableFields.INDEX.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlMetaIndexFieldRecord, Integer> createPositionField() {
    return createField(TableFields.POSITION.fieldName, SQLDataType.INTEGER.nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlMetaIndexFieldRecord, String> createTableRefField() {
    return createField(TableFields.TABLE_REF.fieldName, SQLDataType.VARCHAR
        .nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlMetaIndexFieldRecord, String> createNameField() {
    return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlMetaIndexFieldRecord, FieldIndexOrdering> createOrderingField() {
    return createField(TableFields.ORDERING.fieldName, OrderingConverter.TYPE.nullable(false), this,
        "");
  }
}
