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

package com.torodb.backend.service;

import com.google.common.base.Preconditions;
import com.torodb.backend.BackendLoggerFactory;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.SqlInterface;
import com.torodb.core.TableRefFactory;
import com.torodb.core.backend.MetaInfoKey;
import com.torodb.core.backend.WriteDmlTransaction;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.kvdocument.values.KvValue;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.Collection;

class WriteDmlTransactionImpl extends BackendTransactionImpl implements
    WriteDmlTransaction {
  private static final Logger LOGGER = BackendLoggerFactory.get(WriteDmlTransactionImpl.class);

  public WriteDmlTransactionImpl(
      SqlInterface sqlInterface,
      TableRefFactory tableRefFactory,
      IdentifierFactory identifierFactory,
      KvMetainfoHandler metainfoHandler) {
    super(sqlInterface.getDbBackend().createWriteConnection(), sqlInterface, metainfoHandler);
  }

  @Override
  public int consumeRids(MetaDatabase db, MetaCollection col, MetaDocPart docPart, int howMany) {
    Preconditions.checkState(!isClosed(), "This transaction is closed");

    return getSqlInterface().getMetaDataWriteInterface().consumeRids(getDsl(), db, col, docPart,
        howMany);
  }

  @Override
  public void insert(MetaDatabase db, MetaCollection col, DocPartData data) throws UserException {
    Preconditions.checkState(!isClosed(), "This transaction is closed");

    getSqlInterface().getWriteInterface().insertDocPartData(getDsl(), db.getIdentifier(), data);
  }

  @Override
  public void deleteDids(MetaDatabase db, MetaCollection col, Collection<Integer> dids) {
    Preconditions.checkState(!isClosed(), "This transaction is closed");

    if (dids.isEmpty()) {
      return;
    }

    getSqlInterface().getWriteInterface()
        .deleteCollectionDocParts(getDsl(), db.getIdentifier(), col, dids);
  }

  @Override
  public KvValue<?> writeMetaInfo(MetaInfoKey key, KvValue<?> newValue) {
    return getMetainfoHandler().writeMetaInfo(getDsl(), key, newValue);
  }

  @Override
  public void commit() throws UserException, RollbackException {
    Preconditions.checkState(!isClosed(), "This transaction is closed");

    try {
      getConnection().commit();
    } catch (SQLException ex) {
      getSqlInterface().getErrorHandler().handleUserException(Context.COMMIT, ex);
    } finally {
      getDsl().configuration().connectionProvider().release(getConnection());
    }
  }
}