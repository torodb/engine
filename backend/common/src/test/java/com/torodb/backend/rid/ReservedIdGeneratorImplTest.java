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

package com.torodb.backend.rid;

import static org.junit.Assert.assertEquals;

import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.ReservedIdGenerator.DocPartRidGenerator;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

@RunWith(JUnitPlatform.class)
public class ReservedIdGeneratorImplTest {

  @Test
  public void whenTableRefDoesntExistsCallsToFactory() {
    ReservedIdInfoFactory factory = new MockedReservedIdInfoFactory();

    TableRefFactory tableRefFactory = new TableRefFactoryImpl();
    ReservedIdInfoFactory reservedIdInfoFactory = Mockito.spy(factory);
    ReservedIdGeneratorImpl container = new ReservedIdGeneratorImpl(reservedIdInfoFactory);
    container.load(new ImmutableMetaSnapshot.Builder().build());

    DocPartRidGenerator docPartRidGenerator = container.getDocPartRidGenerator("myDB",
        "myCollection");
    int nextRid = docPartRidGenerator.nextRid(tableRefFactory.createRoot());
    Mockito.verify(reservedIdInfoFactory).create("myDB", "myCollection", tableRefFactory
        .createRoot());
    assertEquals(1, nextRid);

  }

  private static class MockedReservedIdInfoFactory implements ReservedIdInfoFactory {

    @Override
    public void load(MetaSnapshot snapshot) {
    }

    @Override
    public ReservedIdInfo create(String dbName, String collectionName, TableRef tableRef) {
      return new ReservedIdInfo(0, 0);
    }

  }
}
