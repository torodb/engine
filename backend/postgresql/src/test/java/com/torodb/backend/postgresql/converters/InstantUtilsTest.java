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

package com.torodb.backend.postgresql.converters;

import com.torodb.backend.postgresql.converters.util.InstantUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Calendar;
import java.util.TimeZone;

public class InstantUtilsTest {

  private TimeZone currentTimeZone;

  @Before
  public void setUp() {
    currentTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
  }

  @After
  public void tearDown() {
    TimeZone.setDefault(currentTimeZone);
  }

  @Test
  public void testInstantToString() {

    Calendar c = Calendar.getInstance();
    c.set(Calendar.YEAR, 0);
    c.set(Calendar.MONTH, 0);
    c.set(Calendar.DAY_OF_MONTH, 1);
    c.set(Calendar.AM_PM, Calendar.AM);
    c.set(Calendar.HOUR, 10);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);

    String date = InstantUtils.toString(c.getTimeInMillis());
    Assert.assertEquals("Bad date transformation", date, "0001-01-01 10:00:00+00 BC");
  }
}
