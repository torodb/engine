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

package com.torodb.mongodb.repl.sharding.isolation.db;

/**
 *
 */
class Converter {

  private final int underscoresOnShardId;
  private final String shardId;

  public Converter(String shardId) {
    this.shardId = shardId;
    this.underscoresOnShardId = countUnderscores(shardId);
  }

  private static int countUnderscores(String txt) {
    int underscores = 0;
    for (char c : txt.toCharArray()) {
      if (c == '_') {
        underscores++;
      }
    }
    return underscores;
  }

  final String convertDatabaseName(String dbName) {
    return dbName + "_" + shardId;
  }

  final String convertIndexName(String indexName) {
    return indexName + "_" + shardId;
  }

  final boolean isVisibleDatabase(String dbName) {
    return dbName.endsWith("_" + dbName);
  }

  final String unconvertDatabaseName(String convertedDbName) {
    assert isVisibleDatabase(convertedDbName);

    int underscoreIndex = convertedDbName.length();
    for (int i = 0; i < underscoresOnShardId && underscoreIndex != -1; i++) {
      underscoreIndex = convertedDbName.lastIndexOf('_', underscoreIndex);
    }
    if (underscoreIndex != -1) {
      return convertedDbName.substring(0, underscoreIndex);
    }
    throw new IllegalArgumentException("The converting pattern was not found on the given database "
        + "name '" + convertedDbName + "'");
  }

}