package com.torodb.backend.postgresql.converters;

import com.torodb.backend.postgresql.converters.util.InstantUtils;
import java.util.Calendar;
import org.junit.Assert;
import org.junit.Test;

public class InstantUtilsTest {

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
    Assert.assertEquals("Bad date transformation", date, "0001-01-01 10:00:00+01 BC");
  }
}
