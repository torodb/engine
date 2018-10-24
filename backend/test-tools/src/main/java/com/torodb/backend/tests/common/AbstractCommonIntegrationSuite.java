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

import org.junit.jupiter.api.Test;

public abstract class AbstractCommonIntegrationSuite extends AbstractBackendIntegrationSuite {

  @Test
  public void dataInsertModeShouldBeDisabled() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      assertFalse("Data insert mode not disabled by default", 
          context.getSqlInterface().getDbBackend().isOnDataInsertMode(DATABASE_NAME));
    });
  }

  @Test
  public void shouldEnableDataInsertMode() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /* When */
      context.getSqlInterface().getDbBackend().enableDataInsertMode(DATABASE_NAME);

      /* Then */
      assertTrue("Data insert mode not enabled", 
          context.getSqlInterface().getDbBackend().isOnDataInsertMode(DATABASE_NAME));
    });
  }

  @Test
  public void shouldDisableDataInsertMode() throws Exception {
    context.executeOnDbConnectionWithDslContext(dslContext -> {
      /* When */
      context.getSqlInterface().getDbBackend().enableDataInsertMode(DATABASE_NAME);
      context.getSqlInterface().getDbBackend().disableDataInsertMode(DATABASE_NAME);

      /* Then */
      assertFalse("Data insert mode not diabled", 
          context.getSqlInterface().getDbBackend().isOnDataInsertMode(DATABASE_NAME));
    });
  }

}
