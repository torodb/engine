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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.torodb.backend.SqlInterface;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressFBWarnings(
    value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", 
    justification = "it is initialized in setUp()")
public abstract class AbstractCommonIntegrationSuite {

  private static final String DATABASE_NAME = "database_name";

  private SqlInterface sqlInterface;

  private DatabaseTestContext dbTestContext;

  @Before
  public void setUp() throws Exception {
    dbTestContext = getDatabaseTestContext();
    sqlInterface = dbTestContext.getSqlInterface();
    dbTestContext.setupDatabase();
  }

  @After
  public void tearDown() throws Exception {
    dbTestContext.tearDownDatabase();
  }

  protected abstract DatabaseTestContext getDatabaseTestContext();

  @Test
  public void dataInsertModeShouldBeDisabled() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      assertFalse("Data insert mode not disabled by default", 
          sqlInterface.getDbBackend().isOnDataInsertMode(DATABASE_NAME));
    });
  }

  @Test
  public void shouldEnableDataInsertMode() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      /* When */
      sqlInterface.getDbBackend().enableDataInsertMode(DATABASE_NAME);

      /* Then */
      assertTrue("Data insert mode not enabled", 
          sqlInterface.getDbBackend().isOnDataInsertMode(DATABASE_NAME));
    });
  }

  @Test
  public void shouldDisableDataInsertMode() throws Exception {
    dbTestContext.executeOnDbConnectionWithDslContext(dslContext -> {
      /* When */
      sqlInterface.getDbBackend().enableDataInsertMode(DATABASE_NAME);
      sqlInterface.getDbBackend().disableDataInsertMode(DATABASE_NAME);

      /* Then */
      assertFalse("Data insert mode not diabled", 
          sqlInterface.getDbBackend().isOnDataInsertMode(DATABASE_NAME));
    });
  }

}
