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

package com.torodb.mongodb.filters;

import com.torodb.mongodb.language.Namespace;
import com.torodb.mongowp.commands.oplog.DbCmdOplogOperation;
import com.torodb.mongowp.commands.oplog.DbOplogOperation;
import com.torodb.mongowp.commands.oplog.DeleteOplogOperation;
import com.torodb.mongowp.commands.oplog.InsertOplogOperation;
import com.torodb.mongowp.commands.oplog.NoopOplogOperation;
import com.torodb.mongowp.commands.oplog.OplogOperation;
import com.torodb.mongowp.commands.oplog.OplogOperationVisitor;
import com.torodb.mongowp.commands.oplog.UpdateOplogOperation;


/**
 * A {@link OplogOperation} that extract the namespace or database used by the oplog operation and
 * delegates on a {@link NamespaceFilter} or {@link DatabaseFilter}.
 */
public class ByNamespaceOplogOperationFilter implements OplogOperationFilter {

  private final DatabaseFilter databaseFilter;
  private final NamespaceFilter namespaceFilter;
  private final OplogOperationSwitch opSwitch;
  
  public ByNamespaceOplogOperationFilter(DatabaseFilter databaseFilter,
      NamespaceFilter namespaceFilter) {
    this.databaseFilter = databaseFilter;
    this.namespaceFilter = namespaceFilter;
    this.opSwitch = new OplogOperationSwitch();
  }

  @Override
  public FilterResult<OplogOperation> apply(OplogOperation op) {
    return op.accept(opSwitch, null);
  }

  private FilterResult<OplogOperation> filterByNamespace(OplogOperation op, String db, String col) {
    return namespaceFilter.apply(new Namespace(db, col))
        .map(ignore -> ((OplogOperation op2) -> getNamespaceReason(op2, db,col)));
  }

  private String getNamespaceReason(OplogOperation op, String db, String col) {
    return "It affects the filtered namespace " + db + "." + col;
  }

  private FilterResult<OplogOperation> filterByDatabase(OplogOperation op, String db) {
    return databaseFilter.apply(db)
        .map(ignore -> ((OplogOperation op2) -> getDatabaseReason(op2, db)));
  }

  private String getDatabaseReason(OplogOperation op, String db) {
    return "It affects the filtered database " + db;
  }

  private class OplogOperationSwitch
      implements OplogOperationVisitor<FilterResult<OplogOperation>, Void> {

    @Override
    public FilterResult<OplogOperation> visit(DbCmdOplogOperation op, Void arg) {
      return filterByDatabase(op, op.getDatabase());
    }

    @Override
    public FilterResult<OplogOperation> visit(DbOplogOperation op, Void arg) {
      return filterByDatabase(op, op.getDatabase());
    }

    @Override
    public FilterResult<OplogOperation> visit(DeleteOplogOperation op, Void arg) {
      return filterByNamespace(op, op.getDatabase(), op.getCollection());
    }

    @Override
    public FilterResult<OplogOperation> visit(InsertOplogOperation op, Void arg) {
      return filterByNamespace(op, op.getDatabase(), op.getCollection());
    }

    @Override
    public FilterResult<OplogOperation> visit(NoopOplogOperation op, Void arg) {
      return FilterResult.success();
    }

    @Override
    public FilterResult<OplogOperation> visit(UpdateOplogOperation op, Void arg) {
      return filterByNamespace(op, op.getDatabase(), op.getCollection());
    }

  }
}
