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

package com.torodb.backend.ddl;

import com.google.common.base.Preconditions;
import com.torodb.backend.ErrorHandler;
import com.torodb.backend.SqlInterface;
import com.torodb.core.backend.DdlOperationExecutor;
import com.torodb.core.exceptions.InvalidDatabaseException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import org.jooq.DSLContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Stream;

/**
 * A {@link DdlOperationExecutor} that is implemented by composing several interfaces.
 *
 * The idea is to provide a standard implementation for each composing interface but open that to
 * backend specific implementations.
 */
public class ComposedDdlOperationExecutor implements DdlOperationExecutor {

  private boolean closed = false;
  private final SqlInterface sqlInterface;
  private final DSLContext dsl;
  private final DdlOps ddlOps;
  private final Connection connection;

  public ComposedDdlOperationExecutor(SqlInterface sqlInterface, DdlOps ddlOps) {
    this.sqlInterface = sqlInterface;
    this.connection = sqlInterface.getDbBackend().createSystemConnection();
    this.dsl = sqlInterface.getDslContextFactory().createDslContext(connection);
    this.ddlOps = ddlOps;
  }

  private void commit() {
    try {
      connection.commit();
    } catch (SQLException ex) {
      sqlInterface.getErrorHandler().handleException(ErrorHandler.Context.CLOSE, ex);
    }
  }

  @Override
  public void addDatabase(MetaDatabase db) throws RollbackException {
    Preconditions.checkState(!isClosed(), "This operation executor is closed");
    ddlOps.getWriteStructureDdlOps().addDatabase(dsl, db);
    commit();
  }

  @Override
  public void addCollection(MetaDatabase db, MetaCollection newCol) throws RollbackException {
    Preconditions.checkState(!isClosed(), "This operation executor is closed");
    ddlOps.getWriteStructureDdlOps().addCollection(dsl, db, newCol);
    commit();
  }

  @Override
  public void addDocPart(MetaDatabase db, MetaCollection col, MutableMetaDocPart newDocPart,
      boolean addColumns) throws RollbackException, UserException {
    Preconditions.checkState(!isClosed(), "This operation executor is closed");
    ddlOps.getWriteStructureDdlOps().addDocPart(dsl, db, col, newDocPart, addColumns);
    commit();
  }

  @Override
  public void addColumns(MetaDatabase db, MetaCollection col, MutableMetaDocPart docPart,
      Stream<? extends MetaScalar> scalars,
      Stream<? extends MetaField> fields) throws UserException, RollbackException {
    Preconditions.checkState(!isClosed(), "This operation executor is closed");

    ddlOps.getWriteStructureDdlOps().addColumns(dsl, db, col, docPart, scalars, fields);
    commit();
  }

  @Override
  public MetaSnapshot readMetadata() {
    Preconditions.checkState(!isClosed(), "This operation executor is closed");
    return ddlOps.getReadStructureDdlOp().readMetadata(dsl);
  }

  @Override
  public void disableDataImportMode(MetaDatabase db) throws RollbackException {
    Preconditions.checkState(!isClosed(), "This operation executor is closed");
    ddlOps.getDataImportModeDdlOps().disableDataImportMode(dsl, db);
    commit();
  }

  @Override
  public void enableDataImportMode(MetaDatabase db) throws RollbackException {
    Preconditions.checkState(!isClosed(), "This operation executor is closed");
    ddlOps.getDataImportModeDdlOps().enableDataImportMode(dsl, db);
    commit();
  }

  @Override
  public void dropCollection(MetaDatabase db, MetaCollection coll) throws RollbackException {
    Preconditions.checkState(!isClosed(), "This operation executor is closed");
    ddlOps.getWriteStructureDdlOps().dropCollection(dsl, db, coll);
    commit();
  }

  @Override
  public void dropDatabase(MetaDatabase db) throws RollbackException {
    Preconditions.checkState(!isClosed(), "This operation executor is closed");
    ddlOps.getWriteStructureDdlOps().dropDatabase(dsl, db);
    commit();
  }

  @Override
  public void createIndex(MetaDatabase db, MutableMetaCollection col, MetaIndex index) 
      throws UserException {
    Preconditions.checkState(!isClosed(), "This operation executor is closed");
    ddlOps.getCreateIndexDdlOp().createIndex(dsl, db, col, index);
    commit();
  }

  @Override
  public void dropIndex(MetaDatabase db, MutableMetaCollection col, MetaIndex index) {
    Preconditions.checkState(!isClosed(), "This operation executor is closed");
    ddlOps.getDropIndexDdlOp().dropIndex(dsl, db, col, index);
    commit();
  }

  @Override
  public void renameCollection(MetaDatabase fromDb, MetaCollection fromColl,
      MutableMetaDatabase toDb, MutableMetaCollection toColl) throws RollbackException {
    Preconditions.checkState(!isClosed(), "This operation executor is closed");
    ddlOps.getRenameDdlOp().renameCollection(dsl, fromDb, fromColl, toDb, toColl);
    commit();
  }

  @Override
  public void dropAll() throws RollbackException {
    Preconditions.checkState(!isClosed(), "This operation executor is closed");
    sqlInterface.getStructureInterface().dropAll(dsl);
    commit();
  }

  @Override
  public void dropUserData() throws RollbackException {
    Preconditions.checkState(!isClosed(), "This operation executor is closed");
    sqlInterface.getStructureInterface().dropUserData(dsl);
    commit();
  }

  @Override
  public void checkOrCreateMetaDataTables() throws InvalidDatabaseException {
    Preconditions.checkState(!isClosed(), "This operation executor is closed");
    ddlOps.getWriteStructureDdlOps().checkOrCreateMetaDataTables(dsl);
    commit();
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      dsl.close();
      try {
        connection.close();
      } catch (SQLException ex) {
        sqlInterface.getErrorHandler().handleException(ErrorHandler.Context.CLOSE, ex);
      }
    }
  }
}