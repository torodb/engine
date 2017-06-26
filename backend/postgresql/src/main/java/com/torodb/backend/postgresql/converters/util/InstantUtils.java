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
package com.torodb.backend.postgresql.converters.util;

import org.postgresql.PGStatement;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;



/**
 * Utils for convert Instant values
 */
public class InstantUtils {

  private static final char[] ZEROS = {'0', '0', '0', '0', '0', '0', '0', '0', '0'};
  private static final char[][] NUMBERS;

  static {
    // The expected maximum value is 60 (seconds), so 64 is used "just in case"
    NUMBERS = new char[64][];
    for (int i = 0; i < NUMBERS.length; i++) {
      NUMBERS[i] = ((i < 10 ? "0" : "") + Integer.toString(i)).toCharArray();
    }
  }

  private InstantUtils() {
  }


  public static String toString(long value) {
    Timestamp ts = new Timestamp(value);
    return toString(ts);
  }

  public static String toString(Timestamp x) {
    if (x.getTime() == PGStatement.DATE_POSITIVE_INFINITY) {
      return "infinity";
    } else if (x.getTime() == PGStatement.DATE_NEGATIVE_INFINITY) {
      return "-infinity";
    }

    Calendar cal = getCalendar();
    cal.setTime(x);
    StringBuilder sbuild = new StringBuilder();

    appendDate(sbuild, cal);
    sbuild.append(' ');
    appendTime(sbuild, cal, x.getNanos());

    appendTimeZone(sbuild, cal);

    appendEra(sbuild, cal);

    return sbuild.toString();
  }


  /**
   * Get a shared calendar with the default timezone.
   *
   * @return The shared calendar.
   */
  private static Calendar getCalendar() {
    TimeZone timeZone = TimeZone.getDefault();
    Calendar tmp = new GregorianCalendar();
    tmp.setTimeZone(timeZone);
    return tmp;
  }

  private static void appendDate(StringBuilder sb, Calendar cal) {
    int year = cal.get(Calendar.YEAR);
    int month = cal.get(Calendar.MONTH) + 1;
    int day = cal.get(Calendar.DAY_OF_MONTH);
    appendDate(sb, year, month, day);
  }

  private static void appendDate(StringBuilder sb, int year, int month, int day) {
    // always use at least four digits for the year so very
    // early years, like 2, don't get misinterpreted

    int prevLength = sb.length();
    sb.append(year);
    int leadingZerosForYear = 4 - (sb.length() - prevLength);
    if (leadingZerosForYear > 0) {
      sb.insert(prevLength, ZEROS, 0, leadingZerosForYear);
    }

    sb.append('-');
    sb.append(NUMBERS[month]);
    sb.append('-');
    sb.append(NUMBERS[day]);
  }

  private static void appendTime(StringBuilder sb, Calendar cal, int nanos) {
    int hours = cal.get(Calendar.HOUR_OF_DAY);
    int minutes = cal.get(Calendar.MINUTE);
    int seconds = cal.get(Calendar.SECOND);
    appendTime(sb, hours, minutes, seconds, nanos);
  }


  private static void appendTime(StringBuilder sb, int hours, int minutes, int seconds, int nanos) {
    sb.append(NUMBERS[hours]);

    sb.append(':');
    sb.append(NUMBERS[minutes]);

    sb.append(':');
    sb.append(NUMBERS[seconds]);

    if (nanos == 0) {
      return;
    }
    sb.append('.');
    int len = sb.length();
    sb.append(nanos / 1000); // append microseconds
    int needZeros = 6 - (sb.length() - len);
    if (needZeros > 0) {
      sb.insert(len, ZEROS, 0, needZeros);
    }
  }

  private static void appendEra(StringBuilder sb, Calendar cal) {
    if (cal.get(Calendar.ERA) == GregorianCalendar.BC) {
      sb.append(" BC");
    }
  }

  private static void appendTimeZone(StringBuilder sb, Calendar cal) {
    int offset = (cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET)) / 1000;

    appendTimeZone(sb, offset);
  }

  private static void appendTimeZone(StringBuilder sb, int offset) {
    int absoff = Math.abs(offset);
    int hours = absoff / 60 / 60;
    int mins = (absoff - hours * 60 * 60) / 60;
    int secs = absoff - hours * 60 * 60 - mins * 60;

    sb.append((offset >= 0) ? "+" : "-");

    sb.append(NUMBERS[hours]);

    if (mins == 0 && secs == 0) {
      return;
    }
    sb.append(':');

    sb.append(NUMBERS[mins]);
  }


}
