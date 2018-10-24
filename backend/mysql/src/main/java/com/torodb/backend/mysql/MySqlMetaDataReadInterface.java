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

package com.torodb.backend.mysql;

import com.torodb.backend.AbstractMetaDataReadInterface;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.mysql.tables.MySqlKvTable;
import com.torodb.backend.mysql.tables.MySqlMetaCollectionTable;
import com.torodb.backend.mysql.tables.MySqlMetaDatabaseTable;
import com.torodb.backend.mysql.tables.MySqlMetaDocPartIndexColumnTable;
import com.torodb.backend.mysql.tables.MySqlMetaDocPartIndexTable;
import com.torodb.backend.mysql.tables.MySqlMetaDocPartTable;
import com.torodb.backend.mysql.tables.MySqlMetaFieldTable;
import com.torodb.backend.mysql.tables.MySqlMetaIndexFieldTable;
import com.torodb.backend.mysql.tables.MySqlMetaIndexTable;
import com.torodb.backend.mysql.tables.MySqlMetaScalarTable;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class MySqlMetaDataReadInterface extends AbstractMetaDataReadInterface {

  private final SqlHelper sqlHelper;
  private final MySqlMetaDatabaseTable metaDatabaseTable;
  private final MySqlMetaCollectionTable metaCollectionTable;
  private final MySqlMetaDocPartTable metaDocPartTable;
  private final MySqlMetaFieldTable metaFieldTable;
  private final MySqlMetaScalarTable metaScalarTable;
  private final MySqlMetaDocPartIndexTable metaDocPartIndexTable;
  private final MySqlMetaDocPartIndexColumnTable metaDocPartIndexColumnTable;
  private final MySqlMetaIndexTable metaIndexTable;
  private final MySqlMetaIndexFieldTable metaIndexFieldTable;
  private final MySqlKvTable kvTable;

  @Inject
  public MySqlMetaDataReadInterface(SqlHelper sqlHelper) {
    super(MySqlMetaDocPartTable.DOC_PART, sqlHelper);

    this.sqlHelper = sqlHelper;
    this.metaDatabaseTable = MySqlMetaDatabaseTable.DATABASE;
    this.metaCollectionTable = MySqlMetaCollectionTable.COLLECTION;
    this.metaDocPartTable = MySqlMetaDocPartTable.DOC_PART;
    this.metaFieldTable = MySqlMetaFieldTable.FIELD;
    this.metaScalarTable = MySqlMetaScalarTable.SCALAR;
    this.metaDocPartIndexTable = MySqlMetaDocPartIndexTable.DOC_PART_INDEX;
    this.metaDocPartIndexColumnTable = MySqlMetaDocPartIndexColumnTable.DOC_PART_INDEX_COLUMN;
    this.metaIndexTable = MySqlMetaIndexTable.INDEX;
    this.metaIndexFieldTable = MySqlMetaIndexFieldTable.INDEX_FIELD;
    this.kvTable = MySqlKvTable.KV;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public MySqlMetaDatabaseTable getMetaDatabaseTable() {
    return metaDatabaseTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public MySqlMetaCollectionTable getMetaCollectionTable() {
    return metaCollectionTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public MySqlMetaDocPartTable getMetaDocPartTable() {
    return metaDocPartTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public MySqlMetaFieldTable getMetaFieldTable() {
    return metaFieldTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public MySqlMetaScalarTable getMetaScalarTable() {
    return metaScalarTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public MySqlMetaDocPartIndexTable getMetaDocPartIndexTable() {
    return metaDocPartIndexTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public MySqlMetaDocPartIndexColumnTable getMetaDocPartIndexColumnTable() {
    return metaDocPartIndexColumnTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public MySqlMetaIndexTable getMetaIndexTable() {
    return metaIndexTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public MySqlMetaIndexFieldTable getMetaIndexFieldTable() {
    return metaIndexFieldTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public MySqlKvTable getKvTable() {
    return kvTable;
  }

  @Override
  protected String getReadSchemaSizeStatement(String databaseName) {
    return "SELECT sum(pg_total_relation_size(quote_ident(schemaname) || '.' ||"
        + " quote_ident(tablename)))::bigint FROM pg_tables WHERE schemaname = ?";
  }

  @Override
  protected String getReadCollectionSizeStatement() {
    return "SELECT sum(pg_total_relation_size(quote_ident(schemaname) || '.' ||"
        + " quote_ident(tablename)))::bigint "
        + " FROM `" + TorodbSchema.IDENTIFIER + "`.doc_part"
        + " LEFT JOIN pg_tables ON (tablename = doc_part.identifier)"
        + " WHERE doc_part.database = ? AND schemaname = ? AND doc_part.collection = ?";
  }

  @Override
  protected String getReadDocumentsSizeStatement() {
    return "SELECT sum(pg_total_relation_size(quote_ident(schemaname) || '.' ||"
        + " quote_ident(tablename)))::bigint "
        + " FROM `" + TorodbSchema.IDENTIFIER + "`.doc_part"
        + " LEFT JOIN pg_tables ON (tablename = doc_part.identifier)"
        + " WHERE doc_part.database = ? AND schemaname = ? AND doc_part.collection = ?";
  }

  @Override
  protected String getReadIndexSizeStatement(
      String schemaName, String tableName, String indexName) {
    return "SELECT sum(table_size)::bigint from ("
        + "SELECT pg_relation_size(pg_class.oid) AS table_size "
        + "FROM pg_class join pg_indexes "
        + "  on pg_class.relname = pg_indexes.tablename "
        + "WHERE pg_indexes.schemaname = " + sqlHelper.renderVal(schemaName)
        + "  and pg_indexes.indexname = " + sqlHelper.renderVal(indexName)
        + ") as t";
  }
}
