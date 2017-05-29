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
          "create_collection_filtered",
          "deleteIndex",
          "deleteIndexes",
          "doNothing",
          "dropDatabase",
          "dropDatabase_ignored",
          "dropIndex",
          "dropIndexes",
          "drop_collection_filtered",
          "simpleFirstInsert",
          "simpleInsert",
          "insertRepeated",
          "insert_update_add",
          "letschat_upsert",
          "update_array",
          "update_no_upsert",
          "update_upsert",
          "rename_collection_filtered_1",
          "rename_collection_filtered_2",
          "renamecollection_noDropTarget",
          "renamecollection_dropTarget",

          //test that have to fail
          "failing/applyOps",
          "failing/delete_arr_id",
          "failing/delete_doc_id",
          "failing/delete_without_id",
          "failing/emptyCommand",
          "failing/insert_arr_id",
          "failing/insert_doc_id",
          "failing/insert_without_id",
          "failing/unknownCommand",
          "failing/update_arr_id",
          "failing/update_doc_id",
          "failing/update_no_upsert_without_id"
      ).map(name -> "/oplogapplier/" + name + ".json");
    }

  }

}
