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

import com.google.inject.PrivateModule;

import javax.inject.Singleton;

/**
 * The module that binds {@link DdlOps} and its parts.
 *
 * The default implementation can be changed by extending this class and overriding binding methods
 * to bind different instances.
 */
public class DdlOpsModule extends PrivateModule {

  @Override
  protected void configure() {
    expose(DdlOps.class);

    bind(DdlOps.class);

    bindRenameDdlOp();
    bindCreateIndexDdlOp();
    bindDropIndexDdlOp();
    bindDataImportModeDdlOps();
    bindWriteStructureDdlOps();
    bindReadStructureDdlOp();
  }

  protected void bindRenameDdlOp() {
    bind(RenameCollectionDdlOp.class)
        .to(DefaultRenameCollectionDdlOp.class);
  }

  protected void bindCreateIndexDdlOp() {
    bind(CreateIndexDdlOp.class)
        .to(DefaultCreateIndexDdlOp.class);
  }

  protected void bindDropIndexDdlOp() {
    bind(DropIndexDdlOp.class)
        .to(DefaultDropIndexDdlOp.class);
  }

  protected void bindDataImportModeDdlOps() {
    bind(DataImportModeDdlOps.class)
        .to(ConcurrentDataImportModeDdlOps.class)
        .in(Singleton.class);
  }

  protected void bindWriteStructureDdlOps() {
    bind(WriteStructureDdlOps.class)
        .to(DefaultStructureDdlOps.class);
  }

  protected void bindReadStructureDdlOp() {
    bind(ReadStructureDdlOp.class)
        .to(DefaultReadStructure.class);
  }

}