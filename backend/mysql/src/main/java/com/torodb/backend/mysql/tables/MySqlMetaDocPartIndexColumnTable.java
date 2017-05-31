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
import com.torodb.backend.mysql.tables.records.MySqlMetaDocPartIndexColumnRecord;
import com.torodb.backend.tables.MetaDocPartIndexColumnTable;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
public class MySqlMetaDocPartIndexColumnTable 
    extends MetaDocPartIndexColumnTable<String, MySqlMetaDocPartIndexColumnRecord> {

  private static final long serialVersionUID = -426812622031112992L;
  /**
   * The singleton instance of <code>torodb.field_index</code>
   */
  public static final MySqlMetaDocPartIndexColumnTable DOC_PART_INDEX_COLUMN =
      new MySqlMetaDocPartIndexColumnTable();

  @Override
  public Class<MySqlMetaDocPartIndexColumnRecord> getRecordType() {
    return MySqlMetaDocPartIndexColumnRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public MySqlMetaDocPartIndexColumnTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public MySqlMetaDocPartIndexColumnTable(String alias) {
    this(alias, MySqlMetaDocPartIndexColumnTable.DOC_PART_INDEX_COLUMN);
  }

  private MySqlMetaDocPartIndexColumnTable(String alias,
      Table<MySqlMetaDocPartIndexColumnRecord> aliased) {
    this(alias, aliased, null);
  }

  private MySqlMetaDocPartIndexColumnTable(String alias,
      Table<MySqlMetaDocPartIndexColumnRecord> aliased, Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MySqlMetaDocPartIndexColumnTable as(String alias) {
    return new MySqlMetaDocPartIndexColumnTable(alias, this);
  }

  /**
   * Rename this table
   */
  public MySqlMetaDocPartIndexColumnTable rename(String name) {
    return new MySqlMetaDocPartIndexColumnTable(name, null);
  }

  @Override
  protected TableField<MySqlMetaDocPartIndexColumnRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false),
        this, "");
  }

  @Override
  protected TableField<MySqlMetaDocPartIndexColumnRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  @SuppressWarnings("checkstyle:LineLength")
  protected TableField<MySqlMetaDocPartIndexColumnRecord, String> createIndexIdentifierField() {
    return createField(TableFields.INDEX_IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false),
        this, "");
  }

  @Override
  protected TableField<MySqlMetaDocPartIndexColumnRecord, String> createTableRefField() {
    return createField(TableFields.TABLE_REF.fieldName, SQLDataType.VARCHAR
        .nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlMetaDocPartIndexColumnRecord, Integer> createPositionField() {
    return createField(TableFields.POSITION.fieldName, SQLDataType.INTEGER.nullable(false),
        this, "");
  }

  @Override
  protected TableField<MySqlMetaDocPartIndexColumnRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  @SuppressWarnings("checkstyle:LineLength")
  protected TableField<MySqlMetaDocPartIndexColumnRecord, FieldIndexOrdering> createOrderingField() {
    return createField(TableFields.ORDERING.fieldName, OrderingConverter.TYPE.nullable(false), this,
        "");
  }

}
