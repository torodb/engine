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

package com.torodb.mongodb.repl.oplogreplier;

import com.google.common.collect.Streams;
import com.torodb.akka.chronicle.queue.TemporalChronicleQueueFactory;
import com.torodb.mongodb.repl.oplogreplier.batch.OplogBatch;
import com.torodb.mongodb.repl.oplogreplier.utils.OplogOpsParser;
import com.torodb.mongowp.bson.BsonDocument;
import com.torodb.mongowp.bson.org.bson.utils.MongoBsonTranslator;
import com.torodb.mongowp.commands.oplog.OplogOperation;
import net.openhft.chronicle.queue.ChronicleQueue;
import org.jooq.lambda.UncheckedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ContainerExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ObjectArrayArguments;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.UseTechnicalNames;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
@RunWith(JUnitPlatform.class)
@UseTechnicalNames
public class OplogBatchMarshallerTest {

  private final OplogBatchMarshaller marshaller = new OplogBatchMarshaller();
  private ChronicleQueue queue;

  @BeforeEach
  void setUp() {
    queue = TemporalChronicleQueueFactory.createTemporalQueue();
  }

  @ParameterizedTest
  @ArgumentsSource(BatchesProvider.class)
  public void testWriteAndRead(OplogBatch expected) {
    marshaller.write(queue.acquireAppender(), expected);
    OplogBatch result = marshaller.read(queue.createTailer());

    checkExpected(expected, result);

  }

  private void checkExpected(OplogBatch expected, OplogBatch result) {
    Assertions.assertEquals(expected.isLastOne(), result.isLastOne(), "Different lastOne");
    Assertions.assertEquals(expected.isReadyForMore(), result.isReadyForMore(), "Different readyForMore");
    Assertions.assertIterableEquals(expected.getOps(), result.getOps(), "Different operations");
  }

  static class BatchesProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> arguments(ContainerExtensionContext context) {
      Stream<OplogBatch> notReadyForMore = streamOplogOps()
          .map(ops -> new NormalOplogBatch(ops, true));
      Stream<OplogBatch> readyForMore = streamOplogOps()
          .map(ops -> new NormalOplogBatch(ops, false));
      Stream<OplogBatch> specialCases = Stream.of(
          FinishedOplogBatch.getInstance(),
          NotReadyForMoreOplogBatch.getInstance()
      );
      return Streams.concat(notReadyForMore, readyForMore, specialCases)
          .map(ObjectArrayArguments::create);
    }

    private Stream<List<OplogOperation>> streamOplogOps() {
      return streamSampleFileNames()
          .map(BatchesProvider::parse);
    }

    private Stream<String> streamSampleFileNames() {
      return Stream.of(
          "applyOps",
          "delete_1",
          "deleteIndexes",
          "deleteIndex",
          "doNothing",
          "drop_collection",
          "dropDatabase",
          "dropIndexes",
          "dropIndex",
          "emptyCommand",
          "insert_1",
          "letschat_upsert",
          "simple_real_oplog",
          "unknownCommand",
          "update_1",
          "update_2"
      );
    }

    private static List<OplogOperation> parse(String filename) {
      Class<OplogBatchMarshallerTest> clazz = OplogBatchMarshallerTest.class;
      String text;
      try (InputStream resourceAsStream = clazz.getResourceAsStream("/oplogbatch/marshaller/" + filename + ".json");
        BufferedReader reader = new BufferedReader(new InputStreamReader(resourceAsStream))) {
        text = reader.lines().collect(Collectors.joining("\n"));
      } catch (IOException ex) {
        throw new UncheckedException(ex);
      }

      BsonDocument doc = MongoBsonTranslator.translate(
          org.bson.BsonDocument.parse(text)
      );

      return OplogOpsParser.parseOps(doc);
    }

  }

}
