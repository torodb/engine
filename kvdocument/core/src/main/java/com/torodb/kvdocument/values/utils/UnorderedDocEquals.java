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

package com.torodb.kvdocument.values.utils;

import com.torodb.kvdocument.values.KvArray;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.kvdocument.values.KvValueAdaptor;

/**
 *
 */
public class UnorderedDocEquals extends KvValueAdaptor<Boolean, KvValue<?>> {

  private static final ComparisonVisitor COMPARISON_VISITOR = new ComparisonVisitor();

  private UnorderedDocEquals() {}

  public static boolean equals(KvValue<?> v1, KvValue<?> v2) {
    return v1.accept(COMPARISON_VISITOR, v2);
  }

  private static class ComparisonVisitor extends KvValueAdaptor<Boolean, KvValue<?>> {

    @Override
    public Boolean visit(KvDocument value, KvValue<?> other) {
      if (other == value) {
        return true;
      }
      if (other == null) {
        return false;
      }
      if (!(other instanceof KvDocument)) {
        return false;
      }
      KvDocument otherDoc = (KvDocument) other;

      if (value.size() != otherDoc.size()) {
        return false;
      }
      for (KvDocument.DocEntry<?> myEntry : value) {
        KvValue<?> otherValue = otherDoc.get(myEntry.getKey());
        if (otherValue == null) {
          return false;
        }
        if (!otherValue.accept(this, myEntry.getValue())) {
          return false;
        }
      }
      return true;
    }

    @Override
    public Boolean visit(KvArray value, KvValue<?> other) {
      if (other == value) {
        return true;
      }
      if (other == null) {
        return false;
      }
      if (!(other instanceof KvArray)) {
        return false;
      }
      KvArray otherArr = (KvArray) other;

      if (value.size() != otherArr.size()) {
        return false;
      }
      for (int i = 0; i < value.size(); i++) {
        KvValue<?> v1 = value.get(i);
        KvValue<?> v2 = otherArr.get(i);

        if (!v1.accept(this, v2)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public Boolean defaultCase(KvValue<?> value, KvValue<?> other) {
      return value.equals(other);
    }
  }

}
