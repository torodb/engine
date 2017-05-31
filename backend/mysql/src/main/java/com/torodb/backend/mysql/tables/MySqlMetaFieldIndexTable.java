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

import com.torodb.backend.converters.jooq.FieldTypeConverter;
import com.torodb.backend.converters.jooq.OrderingConverter;
import com.torodb.backend.mysql.tables.records.MySqlMetaFieldIndexRecord;
import com.torodb.backend.tables.MetaFieldIndexTable;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.core.transaction.metainf.FieldType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
@SuppressWarnings({"checkstyle:AbbreviationAsWordInName", "checkstyle:AbbreviationAsWordInName",
    "checkstyle:LineLength"})
public class MySqlMetaFieldIndexTable
    extends MetaFieldIndexTable<String, MySqlMetaFieldIndexRecord> {

  private static final long serialVersionUID = -426812622031112992L;
  /**
   * The singleton instance of <code>torodb.field_index</code>
   */
  public static final MySqlMetaFieldIndexTable FIELD_INDEX =
      new MySqlMetaFieldIndexTable();

  @Override
  public Class<MySqlMetaFieldIndexRecord> getRecordType() {
    return MySqlMetaFieldIndexRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public MySqlMetaFieldIndexTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public MySqlMetaFieldIndexTable(String alias) {
    this(alias, MySqlMetaFieldIndexTable.FIELD_INDEX);
  }

  private MySqlMetaFieldIndexTable(String alias, Table<MySqlMetaFieldIndexRecord> aliased) {
    this(alias, aliased, null);
  }

  private MySqlMetaFieldIndexTable(String alias, Table<MySqlMetaFieldIndexRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MySqlMetaFieldIndexTable as(String alias) {
    return new MySqlMetaFieldIndexTable(alias, this);
  }

  /**
   * Rename this table
   */
  public MySqlMetaFieldIndexTable rename(String name) {
    return new MySqlMetaFieldIndexTable(name, null);
  }

  @Override
  protected TableField<MySqlMetaFieldIndexRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlMetaFieldIndexRecord, String> createCollectionField() {
    return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<MySqlMetaFieldIndexRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }

  @Override
  protected TableField<MySqlMetaFieldIndexRecord, String> createTableRefField() {
    return createField(TableFields.TABLE_REF.fieldName, SQLDataType.VARCHAR
        .nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlMetaFieldIndexRecord, Integer> createPositionField() {
    return createField(TableFields.POSITION.fieldName, SQLDataType.INTEGER.nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlMetaFieldIndexRecord, String> createNameField() {
    return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlMetaFieldIndexRecord, FieldType> createTypeField() {
    return createField(TableFields.TYPE.fieldName, FieldTypeConverter.TYPE.nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlMetaFieldIndexRecord, FieldIndexOrdering> createOrderingField() {
    return createField(TableFields.ORDERING.fieldName, OrderingConverter.TYPE.nullable(false), this,
        "");
  }

}
