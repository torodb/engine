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

package com.torodb.torod.exception;

/**
 *
 */
public class UnexistentCollectionException extends SchemaOperationException {

  private static final long serialVersionUID = 6900089823919945L;

  private final String dbName;
  private final String colName;

  public UnexistentCollectionException(String dbName, String colName) {
    this.dbName = dbName;
    this.colName = colName;
  }

  public UnexistentCollectionException(String dbName, String colName, String message) {
    super(message);
    this.dbName = dbName;
    this.colName = colName;
  }

  public UnexistentCollectionException(String dbName, String colName, String message,
      Throwable cause) {
    super(message, cause);
    this.dbName = dbName;
    this.colName = colName;
  }

  public UnexistentCollectionException(String dbName, String colName, Throwable cause) {
    super(cause);
    this.dbName = dbName;
    this.colName = colName;
  }

  public String getDatabaseName() {
    return dbName;
  }

  public String getCollectionName() {
    return colName;
  }
}