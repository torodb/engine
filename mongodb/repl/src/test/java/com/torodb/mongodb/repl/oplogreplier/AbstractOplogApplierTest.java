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


import com.torodb.mongodb.repl.oplogreplier.utils.ClosableContext;
import com.torodb.mongodb.repl.oplogreplier.utils.OplogApplierTest;
import com.torodb.mongodb.repl.oplogreplier.utils.OplogApplierTestFactory;
import com.torodb.mongodb.repl.oplogreplier.utils.ResourceOplogTestStreamer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.function.Supplier;
import java.util.stream.Stream;

@RunWith(JUnitPlatform.class)
public abstract class AbstractOplogApplierTest {
  
  protected String getName(OplogApplierTest oplogTest) {
    return oplogTest.getTestName().orElse("unknown");
  }
  
  protected abstract Supplier<ClosableContext> contextSupplier();

  @TestFactory
  protected Stream<DynamicTest> createJsonTests() throws Exception {
    return oplogTestSupplier()
        .map(oplogTest -> OplogApplierTestFactory.oplogTest(
            getName(oplogTest),
            oplogTest,
            contextSupplier())
        );
  }

  protected Stream<OplogApplierTest> oplogTestSupplier() {
    return new DefaultTestsStreamer().get();
  }

  static class DefaultTestsStreamer extends ResourceOplogTestStreamer {

    @Override
    protected Stream<String> streamFileNames() {
      return Stream.of(
          "createCollectionFiltered",
          "deleteIndex",
          "deleteIndexes",
          "doNothing",
          "dropDatabase",
          "dropDatabaseIgnored",
          "dropCollectionFiltered",
          "simpleFirstInsert",
          "simpleInsert",
          "insertRepeated",
          "insertUpdateAdd",
          "letschatUpsert",
          "updateArray",
          "updateNoUpsert",
          "updateUpsert",
          "renameCollectionFiltered1",
          "renameCollectionFiltered2",
          "renameCollectionNoDropTarget",
          "renameCollectionDropTarget",
          "createIndexesFiltered",

          //test that have to fail
          "failing/applyOps",
          "failing/deleteArrId",
          "failing/deleteDocId",
          "failing/deleteWithoutId",
          "failing/emptyCommand",
          "failing/insertArrId",
          "failing/insertDocId",
          "failing/insertWithoutId",
          "failing/unknownCommand",
          "failing/updateArrId",
          "failing/updateDocId",
          "failing/updateNoUpsertWithoutId"
      ).map(name -> "/oplogapplier/" + name + ".json");
    }

  }

}
