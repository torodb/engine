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

package com.torodb.backend.rid;

import com.torodb.backend.SqlInterface;
import com.torodb.core.TableRef;
import com.torodb.core.exceptions.ToroRuntimeException;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import org.jooq.DSLContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

@ThreadSafe
public class ReservedIdInfoFactoryImpl implements ReservedIdInfoFactory {

  private final SqlInterface sqlInterface;
  @SuppressWarnings("checkstyle:LineLength")
  private ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<TableRef, ReservedIdInfo>>> megaMap;

  @Inject
  public ReservedIdInfoFactoryImpl(SqlInterface sqlInterface)
      throws SQLException {
    this.sqlInterface = sqlInterface;
  }

  @Override
  public void load(MetaSnapshot snapshot) {

    try (Connection connection = sqlInterface.getDbBackend().createSystemConnection()) {
      DSLContext dsl = sqlInterface.getDslContextFactory().createDslContext(connection);

      megaMap = loadRowIds(dsl, snapshot);
    } catch (SQLException ex) {
      throw new ToroRuntimeException("It was impossible to open a connection with the remote "
          + "database", ex);
    }

  }

  @SuppressWarnings("checkstyle:LineLength")
  private ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<TableRef, ReservedIdInfo>>> loadRowIds(
      DSLContext dsl, MetaSnapshot snapshot) {
    ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<TableRef, ReservedIdInfo>>> rowsIdMap =
        new ConcurrentHashMap<>();

    snapshot.streamMetaDatabases().forEach(db -> {
      ConcurrentHashMap<String, ConcurrentHashMap<TableRef, ReservedIdInfo>> collMap =
          new ConcurrentHashMap<>();
      rowsIdMap.put(db.getName(), collMap);
      db.streamMetaCollections().forEach(collection -> {
        ConcurrentHashMap<TableRef, ReservedIdInfo> tableRefMap = new ConcurrentHashMap<>();
        collMap.put(collection.getName(), tableRefMap);
        collection.streamContainedMetaDocParts().forEach(metaDocPart -> {
          TableRef tableRef = metaDocPart.getTableRef();
          Integer lastRowIUsed = sqlInterface.getReadInterface().getLastRowIdUsed(dsl, db,
              collection, metaDocPart);
          tableRefMap.put(tableRef, new ReservedIdInfo(lastRowIUsed, lastRowIUsed));
        });
      });
    });
    return rowsIdMap;
  }

  @Override
  public ReservedIdInfo create(String dbName, String collectionName, TableRef tableRef) {
    ConcurrentHashMap<String, ConcurrentHashMap<TableRef, ReservedIdInfo>> collectionsMap =
        this.megaMap.computeIfAbsent(
            dbName,
            name -> new ConcurrentHashMap<>()
        );
    ConcurrentHashMap<TableRef, ReservedIdInfo> docPartsMap = collectionsMap.computeIfAbsent(
        collectionName,
        name -> new ConcurrentHashMap<>());
    return docPartsMap.computeIfAbsent(tableRef, tr -> new ReservedIdInfo(-1, -1));
  }

}
