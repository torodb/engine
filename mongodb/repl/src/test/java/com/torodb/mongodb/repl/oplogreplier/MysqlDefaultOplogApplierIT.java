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
import com.torodb.mongodb.repl.MysqlDockerSqlCoreMetaBundle;
import com.torodb.mongodb.repl.oplogreplier.utils.BundleClosableContextSupplier;
import com.torodb.mongodb.repl.oplogreplier.utils.ClosableContext;
import com.torodb.mongodb.repl.oplogreplier.utils.DefaultOplogApplierBundleFactory;
import com.torodb.mongodb.repl.oplogreplier.utils.OplogApplierTest;
import com.torodb.testing.docker.mysql.MysqlService;
import com.torodb.testing.docker.mysql.EnumVersion;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.text.MessageFormat;
import java.util.function.Supplier;

/**
 *
 */
public class MysqlDefaultOplogApplierIT extends AbstractOplogApplierTest {

  private static MysqlService mysqlDockerService;

  @BeforeAll
  static void beforeAll() {
    mysqlDockerService = MysqlService.defaultService(EnumVersion.LATEST);
    System.out.println("Starting mysql docker");
    mysqlDockerService.startAsync();
    mysqlDockerService.awaitRunning();
    System.out.println("Mysql docker started");
  }

  @AfterAll
  static void afterAll() {
    if (mysqlDockerService != null && mysqlDockerService.isRunning()) {
      mysqlDockerService.stopAsync();
      mysqlDockerService.awaitTerminated();
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
    return contextSupplier(MysqlDockerSqlCoreMetaBundle.fromDocker(mysqlDockerService));
  }

}
