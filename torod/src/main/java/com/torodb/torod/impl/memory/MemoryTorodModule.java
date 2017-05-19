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

package com.torodb.torod.impl.memory;

import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.MemoryRidGenerator;
import com.torodb.core.d2r.MockIdentifierFactory;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.d2r.impl.D2RModule;
import com.torodb.core.guice.EssentialToDefaultModule;
import com.torodb.torod.ProtectedServer;

public class MemoryTorodModule extends PrivateModule {

  @Override
  protected void configure() {
    expose(ProtectedServer.class);

    install(new EssentialToDefaultModule());
    install(new D2RModule());

    bind(ProtectedServer.class)
        .to(MemoryTorodServer.class)
        .in(Singleton.class);

    bind(IdentifierFactory.class)
        .toInstance(new MockIdentifierFactory());
    bind(ReservedIdGenerator.class)
        .toInstance(new MemoryRidGenerator());
  }

}
