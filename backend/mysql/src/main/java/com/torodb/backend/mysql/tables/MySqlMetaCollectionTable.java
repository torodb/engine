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

import com.torodb.backend.mysql.tables.records.MySqlMetaCollectionRecord;
import com.torodb.backend.tables.MetaCollectionTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS", "HE_HASHCODE_NO_EQUALS"})
@SuppressWarnings({"checkstyle:AbbreviationAsWordInName", "checkstyle:AbbreviationAsWordInName",
    "checkstyle:LineLength"})
public class MySqlMetaCollectionTable extends MetaCollectionTable<MySqlMetaCollectionRecord> {

  private static final long serialVersionUID = 304258902776870571L;
  /**
   * The singleton instance of <code>torodb.collections</code>
   */
  public static final MySqlMetaCollectionTable COLLECTION = new MySqlMetaCollectionTable();

  @Override
  public Class<MySqlMetaCollectionRecord> getRecordType() {
    return MySqlMetaCollectionRecord.class;
  }

  /**
   * Create a <code>torodb.collections</code> table reference
   */
  public MySqlMetaCollectionTable() {
    this(TABLE_NAME, null);
  }

  /**
   * Create an aliased <code>torodb.collections</code> table reference
   */
  public MySqlMetaCollectionTable(String alias) {
    this(alias, MySqlMetaCollectionTable.COLLECTION);
  }

  private MySqlMetaCollectionTable(String alias, Table<MySqlMetaCollectionRecord> aliased) {
    this(alias, aliased, null);
  }

  private MySqlMetaCollectionTable(String alias, Table<MySqlMetaCollectionRecord> aliased,
      Field<?>[] parameters) {
    super(alias, aliased, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MySqlMetaCollectionTable as(String alias) {
    return new MySqlMetaCollectionTable(alias, this);
  }

  /**
   * Rename this table
   */
  public MySqlMetaCollectionTable rename(String name) {
    return new MySqlMetaCollectionTable(name, null);
  }

  @Override
  protected TableField<MySqlMetaCollectionRecord, String> createDatabaseField() {
    return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlMetaCollectionRecord, String> createNameField() {
    return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
  }

  @Override
  protected TableField<MySqlMetaCollectionRecord, String> createIdentifierField() {
    return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this,
        "");
  }
}
