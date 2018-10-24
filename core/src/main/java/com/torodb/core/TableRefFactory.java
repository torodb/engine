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

package com.torodb.core;

import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Key;

import java.util.List;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

/**
 *
 */
@ThreadSafe
public interface TableRefFactory {

  public TableRef createRoot();

  public TableRef createChild(TableRef parent, String name);

  public TableRef createChild(TableRef parent, int arrayDimension);

  /**
   * Translates the given attribute reference into a table ref.
   */
  public default TableRef translate(AttributeReference attRef) {
    TableRef ref = createRoot();

    if (attRef.getKeys().isEmpty()) {
      throw new IllegalArgumentException("The empty attribute reference is not valid");
    }

    Function<Key<?>, String> extractKeyName = (key) -> {
      if (key instanceof AttributeReference.ObjectKey) {
        return ((AttributeReference.ObjectKey) key).getKey();
      } else {
        throw new IllegalArgumentException("Keys whose type is not object are not valid");
      }
    };

    if (attRef.getKeys().size() > 1) {
      List<AttributeReference.Key<?>> keys = attRef.getKeys();
      List<AttributeReference.Key<?>> tableKeys = keys.subList(0, keys.size() - 1);
      for (AttributeReference.Key<?> key : tableKeys) {
        ref = createChild(ref, extractKeyName.apply(key));
      }
    }
    return ref;
  }
}
