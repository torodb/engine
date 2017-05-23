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

package com.torodb.core.d2r;

import com.torodb.core.TableRef;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.d2r.UniqueIdentifierGenerator.IdentifierChecker;
import com.torodb.core.d2r.UniqueIdentifierGenerator.NameChain;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import org.jooq.lambda.tuple.Tuple3;

import javax.inject.Inject;

public class DefaultIdentifierFactory implements IdentifierFactory {

  private final UniqueIdentifierGenerator uniqueIdentifierGenerator;
  private final IdentifierConstraints identifierConstraints;

  @Inject
  public DefaultIdentifierFactory(UniqueIdentifierGenerator uniqueIdentifierGenerator) {
    this.uniqueIdentifierGenerator = uniqueIdentifierGenerator;
    this.identifierConstraints = uniqueIdentifierGenerator.getIdentifierConstraints();
  }

  @Override
  public String toDatabaseIdentifier(MetaSnapshot metaSnapshot, String database) {
    NameChain nameChain = uniqueIdentifierGenerator.createNameChain();
    nameChain.add(database);

    IdentifierChecker uniqueIdentifierChecker = new DatabaseIdentifierChecker(metaSnapshot);

    return uniqueIdentifierGenerator.generateUniqueIdentifier(nameChain, uniqueIdentifierChecker);
  }

  @Override
  public String toCollectionIdentifier(MetaSnapshot metaSnapshot, String database,
      String collection) {
    NameChain nameChain = uniqueIdentifierGenerator.createNameChain();
    nameChain.add(database);
    nameChain.add(collection);

    IdentifierChecker uniqueIdentifierChecker = new CollectionIdentifierChecker(metaSnapshot);

    return uniqueIdentifierGenerator.generateUniqueIdentifier(nameChain, uniqueIdentifierChecker);
  }

  @Override
  public String toDocPartIdentifier(MetaDatabase metaDatabase, String collection,
      TableRef tableRef) {
    NameChain nameChain = uniqueIdentifierGenerator.createNameChain();
    nameChain.add(collection);
    uniqueIdentifierGenerator.append(nameChain, tableRef);

    IdentifierChecker uniqueIdentifierChecker = new TableIdentifierChecker(metaDatabase);

    return uniqueIdentifierGenerator.generateUniqueIdentifier(nameChain, uniqueIdentifierChecker);
  }

  @Override
  public String toFieldIdentifier(MetaDocPart metaDocPart, String field, FieldType fieldType) {
    NameChain nameChain = uniqueIdentifierGenerator.createNameChain();
    nameChain.add(field);

    IdentifierChecker uniqueIdentifierChecker = new FieldIdentifierChecker(metaDocPart);

    return uniqueIdentifierGenerator.generateIdentifier(
        nameChain, uniqueIdentifierChecker, String.valueOf(
        identifierConstraints.getFieldTypeIdentifier(fieldType)));
  }

  @Override
  public String toFieldIdentifierForScalar(FieldType fieldType) {
    return identifierConstraints.getScalarIdentifier(fieldType);
  }

  @Override
  public String toIndexIdentifier(MetaDatabase metaDatabase, String tableName,
      Iterable<Tuple3<String, Boolean, FieldType>> columns) {
    NameChain nameChain = uniqueIdentifierGenerator.createNameChain();
    nameChain.add(tableName);

    for (Tuple3<String, Boolean, FieldType> column : columns) {
      nameChain.add(column.v1());
      nameChain.add(column.v2() ? "a" : "d");
    }

    IdentifierChecker identifierChecker = new IndexIdentifierChecker(metaDatabase);

    return uniqueIdentifierGenerator.generateIdentifier(nameChain, identifierChecker, "idx");
  }

  private static class DatabaseIdentifierChecker implements IdentifierChecker {

    private final MetaSnapshot metaSnapshot;

    public DatabaseIdentifierChecker(MetaSnapshot metaSnapshot) {
      super();
      this.metaSnapshot = metaSnapshot;
    }

    @Override
    public boolean isUnique(String identifier) {
      return metaSnapshot.getMetaDatabaseByIdentifier(identifier) == null;
    }

    @Override
    public boolean isAllowed(IdentifierConstraints identifierInterface, String identifier) {
      return identifierInterface.isAllowedSchemaIdentifier(identifier);
    }
  }

  private static class CollectionIdentifierChecker implements IdentifierChecker {

    private final MetaSnapshot metaSnapshot;

    public CollectionIdentifierChecker(MetaSnapshot metaSnapshot) {
      super();
      this.metaSnapshot = metaSnapshot;
    }

    @Override
    public boolean isUnique(String identifier) {
      return metaSnapshot.streamMetaDatabases().noneMatch(metaDatabase -> metaDatabase
          .getMetaCollectionByIdentifier(identifier) != null);
    }

    @Override
    public boolean isAllowed(IdentifierConstraints identifierInterface, String identifier) {
      return identifierInterface.isAllowedSchemaIdentifier(identifier);
    }
  }

  private static class TableIdentifierChecker implements IdentifierChecker {

    private final MetaDatabase metaDatabase;

    public TableIdentifierChecker(MetaDatabase metaDatabase) {
      super();
      this.metaDatabase = metaDatabase;
    }

    @Override
    public boolean isUnique(String identifier) {
      boolean noDocPartCollision = metaDatabase.streamMetaCollections()
          .allMatch(collection -> collection.getMetaDocPartByIdentifier(identifier) == null);

      boolean noIndexCollision = metaDatabase.streamMetaCollections()
          .flatMap(collection -> collection.streamContainedMetaDocParts())
          .allMatch(docPart -> docPart.getMetaDocPartIndexByIdentifier(identifier) == null);

      return noDocPartCollision && noIndexCollision;
    }

    @Override
    public boolean isAllowed(IdentifierConstraints identifierInterface, String identifier) {
      return identifierInterface.isAllowedTableIdentifier(identifier);
    }
  }

  private static class FieldIdentifierChecker implements IdentifierChecker {

    private final MetaDocPart metaDocPart;

    public FieldIdentifierChecker(MetaDocPart metaDocPart) {
      super();
      this.metaDocPart = metaDocPart;
    }

    @Override
    public boolean isUnique(String identifier) {
      return metaDocPart.getMetaFieldByIdentifier(identifier) == null;
    }

    @Override
    public boolean isAllowed(IdentifierConstraints identifierInterface, String identifier) {
      return identifierInterface.isAllowedColumnIdentifier(identifier);
    }
  }

  private static class IndexIdentifierChecker implements IdentifierChecker {

    private final MetaDatabase metaDatabase;

    public IndexIdentifierChecker(MetaDatabase metaDatabase) {
      super();
      this.metaDatabase = metaDatabase;
    }

    @Override
    public boolean isUnique(String identifier) {
      boolean noDocPartCollision = metaDatabase.streamMetaCollections()
          .allMatch(collection -> collection.getMetaDocPartByIdentifier(identifier) == null);

      boolean noIndexCollision = metaDatabase.streamMetaCollections()
          .flatMap(collection -> collection.streamContainedMetaDocParts())
          .allMatch(docPart -> docPart.getMetaDocPartIndexByIdentifier(identifier) == null);

      return noDocPartCollision && noIndexCollision;
    }

    @Override
    public boolean isAllowed(IdentifierConstraints identifierInterface, String identifier) {
      return identifierInterface.isAllowedIndexIdentifier(identifier);
    }
  }

}
