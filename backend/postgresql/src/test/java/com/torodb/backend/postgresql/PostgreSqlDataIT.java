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

package com.torodb.backend.postgresql;


import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.backend.tests.common.AbstractDataIntegrationSuite;
import com.torodb.backend.tests.common.BackendTestContextFactory;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.WrapperMutableMetaCollection;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.testing.docker.postgres.EnumVersion;
import com.torodb.testing.docker.postgres.PostgresService;
import java.util.ArrayList;
import java.util.List;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class PostgreSqlDataIT extends AbstractDataIntegrationSuite {

  private static PostgresService postgresService;

  @BeforeAll
  public static void beforeAll() {
    postgresService = PostgresService.defaultService(EnumVersion.LATEST);
    postgresService.startAsync();
    postgresService.awaitRunning();
  }

  @AfterAll
  public static void afterAll() {
    if (postgresService != null && postgresService.isRunning()) {
      postgresService.stopAsync();
      postgresService.awaitTerminated();
    }
  }

  @Override
  protected BackendTestContextFactory getBackendTestContextFactory() {
    return new PostgreSqlTestContextFactory(postgresService);
  }


  @ParameterizedTest
  @MethodSource(names = "values")
  public void shouldWriteAndReadDataUsingCopy(
      Tuple2<String, KvValue<?>> labeledValue) throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /* Given */
      FieldType fieldType = FieldType.from(labeledValue.v2.getType());
      createSchema(dslContext);
      createRootTable(dslContext, COLLECTION_NAME);
      DataTypeForKv<?> dataType = context.getSqlInterface()
          .getDataTypeProvider().getDataType(fieldType);
      context.getSqlInterface().getStructureInterface()
          .addColumnToDocPartTable(dslContext, DATABASE_SCHEMA_NAME,
              COLLECTION_NAME, FIELD_COLUMN_NAME + "_"
                  + context.getSqlInterface().getIdentifierConstraints()
                  .getFieldTypeIdentifier(fieldType), dataType);

      ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase
          .Builder(DATABASE_NAME, DATABASE_SCHEMA_NAME)
          .build();
      MutableMetaCollection metaCollection = new WrapperMutableMetaCollection(
          new ImmutableMetaCollection
              .Builder(COLLECTION_NAME, COLLECTION_NAME)
              .build());

      /* When */
      List<KvDocument> newDocs = new ArrayList<>();
      D2RTranslator d2rTranslator = context
          .getD2RTranslatorFactory().createTranslator(metaDatabase, metaCollection);
      for (int i = 0; i < PostgreSqlWriteInterface.MAX_CAPPED_SIZE; i++) {
        KvDocument newDoc = new KvDocument.Builder()
            .putValue(FIELD_COLUMN_NAME, labeledValue.v2)
            .build();
        newDocs.add(newDoc);
        d2rTranslator.translate(newDoc);
      }

      CollectionData collectionData = d2rTranslator.getCollectionDataAccumulator();
      for (DocPartData docPartData : collectionData) {
        context.getSqlInterface().getWriteInterface()
            .insertDocPartData(dslContext, DATABASE_SCHEMA_NAME, docPartData);
      }

      /* Then */
      for (KvDocument newDoc : newDocs) {
        assertThatDataExists(dslContext, metaDatabase, metaCollection, newDoc);
      }
    });
  }


}
