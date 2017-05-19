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

import com.torodb.core.exceptions.InvalidDatabaseException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import org.jooq.DSLContext;

import java.util.stream.Stream;

/**
 *
 */
public interface WriteStructureDdlOps extends AutoCloseable {

  void addDatabase(DSLContext dsl, MetaDatabase db) throws RollbackException;

  void dropDatabase(DSLContext dsl, MetaDatabase db) throws RollbackException;

  void addCollection(DSLContext dsl, MetaDatabase db, MetaCollection newCol)
      throws RollbackException;

  void dropCollection(DSLContext dsl, MetaDatabase db, MetaCollection coll)
      throws RollbackException;

  void addDocPart(DSLContext dsl, MetaDatabase db, MetaCollection col, 
      MutableMetaDocPart newDocPart, boolean addColumns) throws RollbackException, UserException;

  void addColumns(DSLContext dsl, MetaDatabase db, MetaCollection col, MutableMetaDocPart docPart,
      Stream<? extends MetaScalar> scalars,
      Stream<? extends MetaField> fields) throws UserException, RollbackException;

  void checkOrCreateMetaDataTables(DSLContext dsl) throws InvalidDatabaseException;
}