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

import com.google.common.collect.Streams;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import org.jooq.lambda.tuple.Tuple2;

import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A mock identifier factory that accepts all names.
 *
 * Almost all backeds will have restrictions on names, so this class should never be used on real
 * applications. This class has being mainly created to be used on testing.
 */
public class MockIdentifierFactory implements IdentifierFactory {

  private final Function<String, String> nameToIdFun;

  public MockIdentifierFactory() {
    this(Function.identity());
  }

  public MockIdentifierFactory(Function<String, String> nameToIdFun) {
    this.nameToIdFun = nameToIdFun;
  }

  @Override
  public String toDatabaseIdentifier(MetaSnapshot metaSnapshot, String database) {
    return nameToIdFun.apply(database);
  }

  @Override
  public String toCollectionIdentifier(MetaSnapshot metaSnapshot, String database,
      String collection) {
    return nameToIdFun.apply(collection);
  }

  @Override
  public String toDocPartIdentifier(MetaDatabase metaDatabase, String collection,
      TableRef tableRef) {
    return tableRef.toString();
  }

  @Override
  public String toFieldIdentifier(MetaDocPart metaDocPart, String field, FieldType fieldType) {
    return nameToIdFun.apply(field);
  }

  @Override
  public String toFieldIdentifierForScalar(FieldType fieldType) {
    return fieldType.name();
  }

  @Override
  public String toIndexIdentifier(MetaDatabase metaSnapshot, String tableName,
      Iterable<Tuple2<String, Boolean>> identifiers) {
    return Streams.stream(identifiers)
        .map((tuple) -> tuple.v1 + ':' + (tuple.v2 ? "a" : "b")).collect(Collectors.joining("."));
  }

}
