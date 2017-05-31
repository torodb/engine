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

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Types;
import java.text.DecimalFormat;
import java.text.ParseException;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

public class Decimal128ValueConverter implements KvValueConverter<String, String, KvDecimal128> {

  private static final long serialVersionUID = 1L;

  public static final Decimal128ValueConverter CONVERTER =
      new Decimal128ValueConverter();

  public static final DataTypeForKv<KvDecimal128> TYPE =
      DataTypeForKv.from(StringValueConverter.TEXT, CONVERTER, Types.LONGVARCHAR);

  @Override
  public KvType getErasuredType() {
    return JavascriptWithScopeType.INSTANCE;
  }

  @Override
  public KvDecimal128 from(String databaseObject) {
    final JsonReader reader = Json.createReader(new StringReader(databaseObject));
    JsonObject object = reader.readObject();

    if (object.getBoolean("infinity")) {
      return KvDecimal128.getInfinity();
    }

    if (object.getBoolean("NaN")) {
      return KvDecimal128.getNan();
    }

    if (object.getBoolean("negativeZero")) {
      return KvDecimal128.getNegativeZero();
    }

    try {
      return KvDecimal128.of((BigDecimal) DecimalFormat.getNumberInstance()
          .parse(object.getString("value")));
    } catch (ParseException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  @Override
  public String to(KvDecimal128 userObject) {
    return Json.createObjectBuilder()
        .add("infinity", userObject.isInfinite())
        .add("NaN", userObject.isNaN())
        .add("negativeZero", userObject.isNegativeZero())
        .add("value", userObject.getBigDecimal())
        .build().toString();
  }

  @Override
  public Class<String> fromType() {
    return String.class;
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
