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
import com.torodb.kvdocument.values.heap.MapKvDocument;
import com.torodb.kvdocument.values.heap.StringKvString;
import org.jooq.lambda.tuple.Tuple3;
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
      Tuple3<String, KvValue<?>, String> labeledValue) throws Exception {
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
  
  public static List<Tuple3<String, KvValue<?>, String>> values() {
    return ImmutableList.<Tuple3<String,KvValue<?>,String>>of(
          new Tuple3<>("TrueBoolean", KvBoolean.TRUE, "true"),
          new Tuple3<>("FalseBoolean", KvBoolean.FALSE, "false"),
          new Tuple3<>("Null", KvNull.getInstance(), "true"),
          new Tuple3<>("ZeroInteger", KvInteger.of(0), "0"),
          new Tuple3<>("PositiveInteger", KvInteger.of(123), "123"),
          new Tuple3<>("NegativeInteger", KvInteger.of(-3421), "-3421"),
          new Tuple3<>("ZeroDouble", KvDouble.of(0), "0.0"),
          new Tuple3<>("PositiveDouble", KvDouble.of(4.5), "4.5"),
          new Tuple3<>("NegativeDouble", KvDouble.of(-4.5), "-4.5"),
          new Tuple3<>("NormalString", new StringKvString("simple string"), "simple string"),
          new Tuple3<>(
            "StringWithTab",
            new StringKvString("a string with a \t"),
            "a string with a \\\t"
          ),
          new Tuple3<>(
            "StringWithNewLine",
            new StringKvString("a string with a \n"),
            "a string with a \\\n"
          ),
          new Tuple3<>(
            "StringWithBackSlash",
            new StringKvString("a string with a \\"),
            "a string with a \\\\"
          ),
          new Tuple3<>(
            "StringWithSpecials",
            new StringKvString(
                "a string with a \\b, \\f, \\n, \\r, \\t, \\v, \\1, \\12, \\123, \\xa, \\xff"),
                "a string with a \\\\b, \\\\f, \\\\n, \\\\r, \\\\t, \\\\v, \\\\1, \\\\12, "
                + "\\\\123, \\\\xa, \\\\xff"
          ),
          new Tuple3<>(
            "StringNull",
            new StringKvString("a string with a \\N and null literal"),
            "a string with a \\\\N and null literal"
          ),
          new Tuple3<>(
            "MongoObjectId",
            new ByteArrayKvMongoObjectId(
                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc}),
            "\\\\x0102030405060708090A0B0C"
          ),
          new Tuple3<>(
            "DateTime",
            new InstantKvInstant(
                LocalDateTime.of(2015, Month.JANUARY, 18, 2, 43, 26)
                    .toInstant(ZoneOffset.UTC)),
            "'2015-01-18T02:43:26Z'"
          ),
          new Tuple3<>(
            "Date",
            new LocalDateKvDate(LocalDate.of(2015, Month.JANUARY, 18)),
            "'2015-01-18'"
          ),
          new Tuple3<>("Time", new LocalTimeKvTime(LocalTime.of(2, 43, 26)), "'02:43:26'"),
          new Tuple3<>(
              "Binary",
              new ByteSourceKvBinary(
                  KvBinary.KvBinarySubtype.MONGO_GENERIC,
                  Byte.parseByte("1", 2),
                  ByteSource.wrap(new byte[] {0x12, 0x34, 0x56, 0x78, (byte) 0x9a})),
              "\\\\x123456789A"
            ),
          /*new Tuple3<>(
              "BinaryUserDefined",
              new ByteSourceKvBinary(
                  KvBinary.KvBinarySubtype.MONGO_USER_DEFINED,
                  Byte.parseByte("1", 2),
                  ByteSource.wrap(new byte[] {0x12, 0x34, 0x56, 0x78, (byte) 0x9a})),
              "\\\\x123456789A"
            ),*/
          new Tuple3<>("ZeroLong", KvLong.of(0), "0"),
          new Tuple3<>("PositiveLong", KvLong.of(123456789L), "123456789"),
          new Tuple3<>("NegativeLong", KvLong.of(-123456789L), "-123456789"),
          new Tuple3<>("MinKey", KvMinKey.getInstance(), "false"),
          new Tuple3<>("MaxKey", KvMaxKey.getInstance(), "true"),
          new Tuple3<>("Undefined", KvUndefined.getInstance(), "true"),
          new Tuple3<>("Deprecated", KvDeprecated.of("deprecate me"), "deprecate me"),
          new Tuple3<>("Javascript", KvMongoJavascript.of("alert('hello');"), "alert('hello');"),
          new Tuple3<>(
            "JavascriptWithScope",
            KvMongoJavascriptWithScope.of(
                "alert('hello');",
                new MapKvDocument(
                    new LinkedHashMap<>(
                        ImmutableMap.<String, KvValue<?>>builder()
                            .put("a", KvInteger.of(123))
                            .put("b", KvLong.of(0))
                            .build()))),
            "(\"js\":\"alert('hello');\",\"scope\":\"(a : 123, b : 0)\")"
          ),
          new Tuple3<>("ZeroDecimal128", KvDecimal128.of(0, 0), "0." + String.format("%6176s", "")
            .replace(' ', '0')),
          new Tuple3<>("NaNDecimal128", KvDecimal128.of(0x7c00000000000000L, 0), "0." 
            + String.format("%6176s", "").replace(' ', '0')),
          new Tuple3<>("InfiniteDecimal128", KvDecimal128.of(0x7800000000000000L, 0), "0." 
            + String.format("%6176s", "").replace(' ', '0')),
          new Tuple3<>(
            "HighDecimal128",
            KvDecimal128.of(new BigDecimal("1000000000000000000000")),
            "(1000000000000000000000,false,false,false)"
          ),
          new Tuple3<>(
            "NegativeDecimal128",
            KvDecimal128.of(new BigDecimal("-1000000000000000000")),
            "(-1000000000000000000,false,false,false)"
          ),
          new Tuple3<>("TinyDecimal128", KvDecimal128.of(
              new BigDecimal("0.0000000000000000001")), "0.0000000000000000001"),
          new Tuple3<>("MongoRegex", KvMongoRegex.of("pattern", "esd"), 
              "{\"pattern\":\"pattern\",\"options\":\"esd\"}"),
          new Tuple3<>("StrangeMongoRegex", KvMongoRegex.of("pa'tt\"e/rn", "esd"), 
              "(\"pattern\":\"pa'tt\\\\\"e/rn\",\"options\":\"esd\"}"),
          new Tuple3<>("Timestamp", new DefaultKvMongoTimestamp(27, 3), "(27,3)"),
          new Tuple3<>(
            "DbPointer",
            KvMongoDbPointer.of(
                "namespace",
                new ByteArrayKvMongoObjectId(
                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc})),
            "(\"namespace\":\"namespace\",\"objectId\":\"\\\\u0001\\\\u0002"
            + "\\\\u0003\\\\u0004\\\\u0005\\\\u0006\\\\u0007\\\\b\\\\t\\\\n\\\\u000b\\\\f\"}"
          ));
  }
  
}
