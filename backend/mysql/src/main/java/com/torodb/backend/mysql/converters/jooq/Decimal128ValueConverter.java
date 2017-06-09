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

package com.torodb.backend.mysql.converters.jooq;

import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.backend.converters.jooq.KvValueConverter;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.backend.mysql.converters.sql.StringSqlBinding;
import com.torodb.kvdocument.types.JavascriptWithScopeType;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.values.KvDecimal128;

import java.sql.Types;

import javax.json.Json;
import javax.json.JsonObject;

public class Decimal128ValueConverter 
    implements KvValueConverter<JsonObject, String, KvDecimal128> {

  private static final long serialVersionUID = 1L;

  public static final DataTypeForKv<KvDecimal128> TYPE =
      DataTypeForKv.from(JsonConverter.JSON, new Decimal128ValueConverter(), Types.LONGVARCHAR);

  @Override
  public KvType getErasuredType() {
    return JavascriptWithScopeType.INSTANCE;
  }

  @Override
  public KvDecimal128 from(JsonObject databaseObject) {
    if (databaseObject.getBoolean("infinity")) {
      return KvDecimal128.getInfinity();
    }

    if (databaseObject.getBoolean("nan")) {
      return KvDecimal128.getNan();
    }

    if (databaseObject.getBoolean("negativeZero")) {
      return KvDecimal128.getNegativeZero();
    }

    return KvDecimal128.of(databaseObject.getJsonNumber("value").bigDecimalValue());
  }

  @Override
  public JsonObject to(KvDecimal128 userObject) {
    return Json.createObjectBuilder()
        .add("infinity", userObject.isInfinite() && !userObject.isNaN())
        .add("nan", userObject.isNaN())
        .add("negativeZero", userObject.isNegativeZero())
        .add("value", userObject.getBigDecimal())
        .build();
  }

  @Override
  public Class<JsonObject> fromType() {
    return JsonObject.class;
  }

  @Override
  public Class<KvDecimal128> toType() {
    return KvDecimal128.class;
  }

  @Override
  public SqlBinding<String> getSqlBinding() {
    return StringSqlBinding.INSTANCE;
  }
}
