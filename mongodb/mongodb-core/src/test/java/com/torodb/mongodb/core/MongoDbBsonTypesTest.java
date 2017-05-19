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

package com.torodb.mongodb.core;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.bundle.BundleConfigImpl;
import com.torodb.core.guice.EssentialModule;
import com.torodb.core.logging.DefaultLoggerFactory;
import com.torodb.core.metrics.DisabledMetricRegistry;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.mongodb.commands.impl.CommandClassifierImpl;
import com.torodb.mongodb.commands.signatures.general.FindCommand;
import com.torodb.mongodb.commands.signatures.general.InsertCommand;
import com.torodb.mongowp.Status;
import com.torodb.mongowp.bson.BinarySubtype;
import com.torodb.mongowp.bson.BsonArray;
import com.torodb.mongowp.bson.BsonDocument;
import com.torodb.mongowp.bson.BsonType;
import com.torodb.mongowp.bson.BsonValue;
import com.torodb.mongowp.bson.impl.ByteArrayBsonBinary;
import com.torodb.mongowp.bson.impl.DefaultBsonDbPointer;
import com.torodb.mongowp.bson.impl.DefaultBsonJavaScript;
import com.torodb.mongowp.bson.impl.DefaultBsonRegex;
import com.torodb.mongowp.bson.impl.DefaultBsonTimestamp;
import com.torodb.mongowp.bson.impl.InstantBsonDateTime;
import com.torodb.mongowp.bson.impl.IntBasedBsonObjectId;
import com.torodb.mongowp.bson.impl.LongBsonDateTime;
import com.torodb.mongowp.bson.impl.LongsBsonDecimal128;
import com.torodb.mongowp.bson.impl.PrimitiveBsonDouble;
import com.torodb.mongowp.bson.impl.PrimitiveBsonInt32;
import com.torodb.mongowp.bson.impl.PrimitiveBsonInt64;
import com.torodb.mongowp.bson.impl.SimpleBsonMax;
import com.torodb.mongowp.bson.impl.SimpleBsonMin;
import com.torodb.mongowp.bson.impl.SimpleBsonNull;
import com.torodb.mongowp.bson.impl.SimpleBsonUndefined;
import com.torodb.mongowp.bson.impl.SingleEntryBsonDocument;
import com.torodb.mongowp.bson.impl.SingleValueBsonArray;
import com.torodb.mongowp.bson.impl.StringBsonDeprecated;
import com.torodb.mongowp.bson.impl.StringBsonString;
import com.torodb.mongowp.bson.impl.TrueBsonBoolean;
import com.torodb.mongowp.bson.utils.DefaultBsonValues;
import com.torodb.mongowp.commands.Request;
import com.torodb.mongowp.commands.impl.NameBasedCommandLibrary;
import com.torodb.mongowp.utils.BsonDocumentBuilder;
import com.torodb.torod.impl.memory.MemoryTorodBundle;
import com.torodb.torod.TorodBundle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RunWith(Parameterized.class)
public class MongoDbBsonTypesTest {

  private BundleConfig generalConfig;
  private TorodBundle torodBundle;
  private MongoDbCoreBundle bundle;
  private Request request;
  private final String dbName="test";

  @Parameterized.Parameter(0)
  public String collName;


  @Parameterized.Parameter(1)
  public BsonValue value;

  @Before
  public void setUp() {
    Supervisor supervisor = new Supervisor() {
      @Override
      public SupervisorDecision onError(Object supervised, Throwable error) {
        throw new AssertionError("error on " + supervised, error);
      }
    };
    Injector essentialInjector = Guice.createInjector(
        new EssentialModule(
            DefaultLoggerFactory.getInstance(),
            () -> true,
            Clock.systemUTC()
        )
    );

    generalConfig = new BundleConfigImpl(essentialInjector, supervisor);
    torodBundle = new MemoryTorodBundle(generalConfig);

    torodBundle.startAsync();
    torodBundle.awaitRunning();

    MongoDbCoreConfig config = new MongoDbCoreConfig(
        torodBundle,
        new NameBasedCommandLibrary("test", ImmutableMap.of()),
        CommandClassifierImpl.createDefault(
            DefaultLoggerFactory.getInstance(),
            Clock.systemUTC(),
            new DefaultBuildProperties(),
            new MongodServerConfig(HostAndPort.fromParts("localhost", 8095))
        ),
        Optional.of(new DisabledMetricRegistry()),
        DefaultLoggerFactory.getInstance(),
        essentialInjector,
        supervisor
    );

    bundle = new MongoDbCoreBundle(config);

    request = new Request(dbName, null, true, null);

    bundle.start();
  }

  @After
  public void tearDown() {
    if (bundle != null) {
      bundle.stop();
    }

    if (torodBundle != null && torodBundle.isRunning()) {
      torodBundle.stopAsync();
    }
  }

  @Parameterized.Parameters(name = "{index}: Collection {0}")
  public static Collection<Object[]> data() {

    Collection<Object[]> allTests = Arrays.asList(new Object[][] {
            {"DOUBLE", PrimitiveBsonDouble.newInstance(2.3)},
            {"STRING", new StringBsonString("hello")},
            {"DOCUMENT", new SingleEntryBsonDocument("salutation", new StringBsonString("hello"))},
            {"ARRAY", new SingleValueBsonArray(new StringBsonString("hello"))},
            {"BINARY", new ByteArrayBsonBinary(BinarySubtype.USER_DEFINED, Byte.parseByte("1000", 2), new byte [1])},
            {"UNDEFINED", SimpleBsonUndefined.getInstance()},
            {"OBJECT_ID", new IntBasedBsonObjectId(1, 1, 1, 1)},
            {"BOOLEAN", TrueBsonBoolean.getInstance()},
            {"DATETIME", new LongBsonDateTime(0L)},
            {"DATETIME", new InstantBsonDateTime(Instant.now())},
            {"NULL", SimpleBsonNull.getInstance()},
            {"REGEX", new DefaultBsonRegex("", "asd")},
            {"REGEX_WITH_OPTIONS", new DefaultBsonRegex("gimy", "asd")},
            {"DB_POINTER", new DefaultBsonDbPointer("asd", new IntBasedBsonObjectId(1, 1, 1, 1))},
            {"JAVA_SCRIPT", new DefaultBsonJavaScript("alert(\"hello\");")},
            {"DEPRECATED", new StringBsonDeprecated("Deprecable me")},
            //{"JAVA_SCRIPT_WITH_SCOPE", new DefaultBsonJavaScriptWithCode("alert(\"hello\");", new SingleEntryBsonDocument("class", new StringBsonString(".main-menu")))},
            {"INT32", PrimitiveBsonInt32.newInstance(55)},
            {"TIMESTAMP", new DefaultBsonTimestamp(1482000000, 1)},
            {"INT64", PrimitiveBsonInt64.newInstance(1525155)},
            {"DECIMAL128", new LongsBsonDecimal128(1L,1L)},
            {"DECIMAL128_NaN", new LongsBsonDecimal128(8935141660703064064L,1L)},
            {"DECIMAL128_INFINITE", new LongsBsonDecimal128(8646911284551352320L,1L)},
            {"MIN", SimpleBsonMin.getInstance()},
            {"MAX", SimpleBsonMax.getInstance()}
    });

    Arrays.asList(BsonType.values()).forEach(type -> {
      if (type != BsonType.JAVA_SCRIPT_WITH_SCOPE) {
        assertTrue(type + " type is never tested", allTests.stream().anyMatch(
            toTest -> type.getValueClass().isAssignableFrom(toTest[1].getClass())
        ));
      }
    });

    return allTests;
  }

  @Test
  public void test() throws InterruptedException, TimeoutException {
    List<BsonDocument> docs = new ArrayList<>();
    docs.add(new BsonDocumentBuilder()
        .appendUnsafe("number", value)
        .appendUnsafe("_id", DefaultBsonValues.INT32_ONE)
        .build()
    );

    baseWriteTest(docs, collName);
  }

  private void baseWriteTest(List<BsonDocument> docs, String collName)
      throws InterruptedException, TimeoutException {

    try (WriteMongodTransaction writeTransaction = bundle.getExternalInterface()
        .getMongodServer()
        .openWriteTransaction(1, TimeUnit.MINUTES)) {

      //Prepare and do the insertion
      InsertCommand.InsertArgument insertArgument = new InsertCommand.InsertArgument.Builder(
          collName)
          .addDocuments(docs)
          .build();
      Status<InsertCommand.InsertResult> insertResultStatus =
          writeTransaction.execute(request, InsertCommand.INSTANCE, insertArgument);

      //Prepare and do the retrieval
      FindCommand.FindArgument findArgument = new FindCommand.FindArgument.Builder()
          .setCollection(collName)
          .build();
      Status<FindCommand.FindResult> findResultStatus =
          writeTransaction.execute(request, FindCommand.INSTANCE, findArgument);

      writeTransaction.close();

      //Asserting that the status of both operations are right
      assertTrue(insertResultStatus.getResult().getWriteErrors().isEmpty());

      if (!insertResultStatus.isOk()) {
        fail(insertResultStatus.getErrorMsg() + " in collection " + collName);
      }

      if (!findResultStatus.isOk()) {
        fail(findResultStatus.getErrorMsg() + " in collection " + collName);
      }

      //We ensure that every element in the inserted list exists in the list retrieved from DB and viceversa
      //That is, we ensure that (apart from the order) they are the same
      BsonArray result = findResultStatus.getResult()
          .getCursor()
          .marshall(elm -> elm)
          .get("firstBatch")
          .asArray();

      result.forEach(doc -> assertTrue(
          "Generated value " + doc + " is not contained on input data " + docs,
          docs.contains(doc))
      );

      docs.forEach(doc -> assertTrue(
          "Input value " + doc + " is not contained on the generated data " + result,
          result.contains(doc))
      );
    }
  }
}
