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

package com.torodb.kvdocument.values;

import javax.annotation.Nonnull;

public abstract class KVAbstractJavascript extends KvValue<String>{

    private static final long serialVersionUID = 4677607234543862220L;
    protected String value;

    protected KVAbstractJavascript(String value)
    {
        this.value = value;
    }

    @Nonnull
    @Override
    public String getValue() {
        return value;
    }

    @Override
    public Class<? extends String> getValueClass() {
        return String.class;
    }

    @Override
    public String toString() {
        return getValue();
    }

    @Override
    public int hashCode() {
        return getValue().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof KVAbstractJavascript)) {
            return false;
        }
        return this.getValue().equals(((KVAbstractJavascript) obj).getValue());
    }

}
