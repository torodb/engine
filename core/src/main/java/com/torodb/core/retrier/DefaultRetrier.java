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

package com.torodb.core.retrier;

/**
 *
 */
public class DefaultRetrier extends SmartRetrier {

  private static final int MAX_CRITICAL_ATTEMPTS = 100;
  private static final int MAX_INFREQUENT_ATTEMPTS = 5;
  private static final int MAX_FREQUENT_ATTEMPTS = 100;
  private static final int MAX_DEFAULT_ATTEMPTS = 10;

  @SuppressWarnings("checkstyle:indentation")
  private DefaultRetrier() {
    super(
        attempts -> attempts >= MAX_CRITICAL_ATTEMPTS,
        attempts -> attempts >= MAX_INFREQUENT_ATTEMPTS,
        attempts -> attempts >= MAX_FREQUENT_ATTEMPTS,
        attempts -> attempts >= MAX_DEFAULT_ATTEMPTS,
        DefaultRetrier::millisToWait
    );
  }

  public static DefaultRetrier getInstance() {
    return DefaultRetrierHolder.INSTANCE;
  }

  private static int millisToWait(int attempts, int millis) {
    if (millis >= 2000) {
      return 2000;
    }
    int factor = (int) Math.round(millis * (1.5 + Math.random()));
    if (factor < 2) {
      assert millis <= 1;
      factor = 2;
    }
    return Math.min(2000, millis * factor);
  }

  private static class DefaultRetrierHolder {

    private static final DefaultRetrier INSTANCE = new DefaultRetrier();
  }

  //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
  private Object readResolve() {
    return DefaultRetrier.getInstance();
  }
}