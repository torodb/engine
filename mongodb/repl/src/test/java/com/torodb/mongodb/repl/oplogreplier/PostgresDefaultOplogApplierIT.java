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

import com.torodb.mongodb.repl.AbstractCoreMetaBundle;
import com.torodb.mongodb.repl.PostgresDockerSqlCoreMetaBundle;
import com.torodb.mongodb.repl.oplogreplier.utils.BundleClosableContextSupplier;
import com.torodb.mongodb.repl.oplogreplier.utils.ClosableContext;
import com.torodb.mongodb.repl.oplogreplier.utils.DefaultOplogApplierBundleFactory;
import com.torodb.mongodb.repl.oplogreplier.utils.OplogApplierTest;
import com.torodb.testing.docker.postgres.EnumVersion;
import com.torodb.testing.docker.postgres.PostgresService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.text.MessageFormat;
import java.util.function.Supplier;

/**
 *
 */
public class PostgresDefaultOplogApplierIT extends AbstractOplogApplierTest {

  private static PostgresService postgresDockerService;

  @BeforeAll
  static void beforeAll() {
    postgresDockerService = PostgresService.defaultService(EnumVersion.LATEST);
    System.out.println("Starting postgres docker");
    postgresDockerService.startAsync();
    postgresDockerService.awaitRunning();
    System.out.println("Postgres docker started");
  }

  @AfterAll
  static void afterAll() {
    if (postgresDockerService != null && postgresDockerService.isRunning()) {
      postgresDockerService.stopAsync();
      postgresDockerService.awaitTerminated();
    }
  }

  protected Supplier<ClosableContext> contextSupplier(AbstractCoreMetaBundle coreMetaBundle) {
    return new BundleClosableContextSupplier(
        coreMetaBundle,
        new DefaultOplogApplierBundleFactory()
    );
  }

  @Override
  protected String getName(OplogApplierTest oplogTest) {
    return MessageFormat.format("Postgres{0}: {1}",
        EnumVersion.LATEST, super.getName(oplogTest));
  }

  @Override
  protected Supplier<ClosableContext> contextSupplier() {
    return contextSupplier(PostgresDockerSqlCoreMetaBundle.fromDocker(postgresDockerService));
  }

}
