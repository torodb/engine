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

package com.torodb.backend.tests.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;
import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.WrapperMutableMetaCollection;
import com.torodb.kvdocument.values.KvBinary;
import com.torodb.kvdocument.values.KvBoolean;
import com.torodb.kvdocument.values.KvDecimal128;
import com.torodb.kvdocument.values.KvDeprecated;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvDouble;
import com.torodb.kvdocument.values.KvInteger;
import com.torodb.kvdocument.values.KvLong;
import com.torodb.kvdocument.values.KvMaxKey;
import com.torodb.kvdocument.values.KvMinKey;
import com.torodb.kvdocument.values.KvMongoDbPointer;
import com.torodb.kvdocument.values.KvMongoJavascript;
import com.torodb.kvdocument.values.KvMongoJavascriptWithScope;
import com.torodb.kvdocument.values.KvMongoRegex;
import com.torodb.kvdocument.values.KvNull;
import com.torodb.kvdocument.values.KvUndefined;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.kvdocument.values.heap.ByteArrayKvMongoObjectId;
import com.torodb.kvdocument.values.heap.ByteSourceKvBinary;
import com.torodb.kvdocument.values.heap.DefaultKvMongoTimestamp;
import com.torodb.kvdocument.values.heap.InstantKvInstant;
import com.torodb.kvdocument.values.heap.LocalDateKvDate;
import com.torodb.kvdocument.values.heap.LocalTimeKvTime;
import com.torodb.kvdocument.values.heap.LongKvInstant;
import com.torodb.kvdocument.values.heap.MapKvDocument;
import com.torodb.kvdocument.values.heap.StringKvString;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;

public abstract class AbstractDataIntegrationSuite extends AbstractBackendIntegrationSuite {

  @ParameterizedTest
  @MethodSource(names = "values")
  public void shouldWriteAndReadData(
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
      KvDocument newDoc = new KvDocument.Builder()
          .putValue(FIELD_COLUMN_NAME, labeledValue.v2)
          .build();
      D2RTranslator d2rTranslator = context
          .getD2RTranslatorFactory().createTranslator(metaDatabase, metaCollection);
      d2rTranslator.translate(newDoc);
      CollectionData collectionData = d2rTranslator.getCollectionDataAccumulator();
      for (DocPartData docPartData : collectionData) {
        context.getSqlInterface().getWriteInterface()
            .insertDocPartData(dslContext, DATABASE_SCHEMA_NAME, docPartData);
      }
      
      /* Then */
      assertThatDataExists(dslContext, metaDatabase, metaCollection, newDoc);
    });
  }
  
  public static List<Tuple2<String, KvValue<?>>> values() {
    return ImmutableList.<Tuple2<String,KvValue<?>>>of(
          new Tuple2<>("TrueBoolean", KvBoolean.TRUE),
          new Tuple2<>("FalseBoolean", KvBoolean.FALSE),
          new Tuple2<>("Null", KvNull.getInstance()),
          new Tuple2<>("ZeroInteger", KvInteger.of(0)),
          new Tuple2<>("PositiveInteger", KvInteger.of(123)),
          new Tuple2<>("NegativeInteger", KvInteger.of(-3421)),
          new Tuple2<>("ZeroDouble", KvDouble.of(0)),
          new Tuple2<>("PositiveDouble", KvDouble.of(4.5)),
          new Tuple2<>("NegativeDouble", KvDouble.of(-4.5)),
          new Tuple2<>("NormalString", new StringKvString("simple string")),
          new Tuple2<>(
            "StringWithTab",
            new StringKvString("a string with a \t")
          ),
          new Tuple2<>(
            "StringWithNewLine",
            new StringKvString("a string with a \n")
          ),
          new Tuple2<>(
            "StringWithBackSlash",
            new StringKvString("a string with a \\")
          ),
          new Tuple2<>(
            "StringWithSpecials",
            new StringKvString(
                "a string with a \\b, \\f, \\n, \\r, \\t, \\v, \\1, \\12, \\123, \\xa, \\xff")
          ),
          new Tuple2<>(
            "StringNull",
            new StringKvString("a string with a \\N and null literal")
          ),
          new Tuple2<>(
            "MongoObjectId",
            new ByteArrayKvMongoObjectId(
                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc})
          ),
          new Tuple2<>(
              "Instant",
              new InstantKvInstant(
                  LocalDateTime.of(2015, Month.JANUARY, 18, 2, 43, 26)
                      .toInstant(ZoneOffset.UTC))
            ),
          new Tuple2<>(
              "InstantZero",
              new LongKvInstant(-2000L * 365 * 24 * 60 * 60 * 1000)
            ),
          new Tuple2<>(
              "InstantLow",
              new LongKvInstant(Integer.MIN_VALUE)
            ),
          new Tuple2<>(
              "InstantHigh",
              new LongKvInstant(Integer.MAX_VALUE)
            ),
          new Tuple2<>(
            "Date",
            new LocalDateKvDate(LocalDate.of(2015, Month.JANUARY, 18))
          ),
          new Tuple2<>("Time", new LocalTimeKvTime(LocalTime.of(2, 43, 26))),
          new Tuple2<>(
              "Binary",
              new ByteSourceKvBinary(
                  KvBinary.KvBinarySubtype.MONGO_GENERIC,
                  Byte.parseByte("1", 2),
                  ByteSource.wrap(new byte[] {0x12, 0x34, 0x56, 0x78, (byte) 0x9a}))
            ),
          /*new Tuple2<>(
              "BinaryUserDefined",
              new ByteSourceKvBinary(
                  KvBinary.KvBinarySubtype.MONGO_USER_DEFINED,
                  Byte.parseByte("1", 2),
                  ByteSource.wrap(new byte[] {0x12, 0x34, 0x56, 0x78, (byte) 0x9a}))
            ),*/
          new Tuple2<>("ZeroLong", KvLong.of(0)),
          new Tuple2<>("PositiveLong", KvLong.of(123456789L)),
          new Tuple2<>("NegativeLong", KvLong.of(-123456789L)),
          new Tuple2<>("MinKey", KvMinKey.getInstance()),
          new Tuple2<>("MaxKey", KvMaxKey.getInstance()),
          new Tuple2<>("Undefined", KvUndefined.getInstance()),
          new Tuple2<>("Deprecated", KvDeprecated.of("deprecate me")),
          new Tuple2<>("Javascript", KvMongoJavascript.of("alert('hello');")),
          new Tuple2<>(
            "JavascriptWithScope",
            KvMongoJavascriptWithScope.of(
                "alert('hello');",
                new MapKvDocument(
                    new LinkedHashMap<>(
                        ImmutableMap.<String, KvValue<?>>builder()
                            .put("a", KvInteger.of(123))
                            .put("b", KvLong.of(0))
                            .build())))
          ),
          new Tuple2<>("ZeroDecimal128", KvDecimal128.of(0, 0)),
          new Tuple2<>("NaNDecimal128", KvDecimal128.of(0x7c00000000000000L, 0)),
          new Tuple2<>("InfiniteDecimal128", KvDecimal128.of(0x7800000000000000L, 0)),
          new Tuple2<>(
            "HighDecimal128",
            KvDecimal128.of(new BigDecimal("1000000000000000000000"))
          ),
          new Tuple2<>(
            "NegativeDecimal128",
            KvDecimal128.of(new BigDecimal("-1000000000000000000"))
          ),
          new Tuple2<>("TinyDecimal128", KvDecimal128.of(
              new BigDecimal("0.0000000000000000001"))),
          new Tuple2<>("MongoRegex", KvMongoRegex.of("pattern", "esd")),
          new Tuple2<>("StrangeMongoRegex", KvMongoRegex.of("pa'tt\"e/rn", "esd")),
          new Tuple2<>("Timestamp", new DefaultKvMongoTimestamp(27, 3)),
          new Tuple2<>(
            "DbPointer",
            KvMongoDbPointer.of(
                "namespace",
                new ByteArrayKvMongoObjectId(
                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc}))
          ));
  }
  
}
