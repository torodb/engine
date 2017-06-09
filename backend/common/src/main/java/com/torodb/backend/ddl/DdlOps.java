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

import com.google.common.collect.Lists;

import java.util.List;

import javax.inject.Inject;


/**
 *
 */
public class DdlOps implements AutoCloseable {
  private final RenameCollectionDdlOp renameDdlOp;
  private final CreateIndexDdlOp createIndexDdlOp;
  private final DropIndexDdlOp dropIndexDdlOp;
  private final DataImportModeDdlOps dataImportModeDdlOps;
  private final WriteStructureDdlOps structureDdlOps;
  private final ReadStructureDdlOp readStructureDdlOp;

  @Inject
  public DdlOps(RenameCollectionDdlOp renameDdlOp, CreateIndexDdlOp createIndexDdlOp,
      DropIndexDdlOp dropIndexDdlOp, DataImportModeDdlOps dataImportModeDdlOps,
      WriteStructureDdlOps writeStructureDdlOps, ReadStructureDdlOp readStructureDdlOp) {
    this.renameDdlOp = renameDdlOp;
    this.createIndexDdlOp = createIndexDdlOp;
    this.dropIndexDdlOp = dropIndexDdlOp;
    this.dataImportModeDdlOps = dataImportModeDdlOps;
    this.structureDdlOps = writeStructureDdlOps;
    this.readStructureDdlOp = readStructureDdlOp;
  }

  public RenameCollectionDdlOp getRenameDdlOp() {
    return renameDdlOp;
  }

  public CreateIndexDdlOp getCreateIndexDdlOp() {
    return createIndexDdlOp;
  }

  public DropIndexDdlOp getDropIndexDdlOp() {
    return dropIndexDdlOp;
  }

  public DataImportModeDdlOps getDataImportModeDdlOps() {
    return dataImportModeDdlOps;
  }

  public WriteStructureDdlOps getWriteStructureDdlOps() {
    return structureDdlOps;
  }

  public ReadStructureDdlOp getReadStructureDdlOp() {
    return readStructureDdlOp;
  }

  @Override
  public void close() throws Exception {
    List<AutoCloseable> autoCloseables = Lists.newArrayList(
        renameDdlOp,
        createIndexDdlOp,
        dropIndexDdlOp,
        dataImportModeDdlOps,
        structureDdlOps,
        readStructureDdlOp);
    Exception ex = null;
    for (AutoCloseable autoCloseable : autoCloseables) {
      try {
        autoCloseable.close();
      } catch (Exception ex2) {
        if (ex == null) {
          ex = ex2;
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

}