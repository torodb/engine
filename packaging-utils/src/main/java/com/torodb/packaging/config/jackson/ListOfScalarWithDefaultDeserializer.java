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

package com.torodb.packaging.config.jackson;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.torodb.packaging.config.model.common.ListOfScalarWithDefault;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;

public abstract class ListOfScalarWithDefaultDeserializer<T>
    extends JsonDeserializer<ListOfScalarWithDefault<T>> {

  private final Class<? extends ListOfScalarWithDefault<T>> listOfScalarWithDefaulClass;
  private final Class<T> listOfScalarImplementationClass;
  private final TypeReference<List<T>> listOfScalarTypeReference;
  
  public <SWC extends ListOfScalarWithDefault<T>> ListOfScalarWithDefaultDeserializer(
      Class<SWC> scalarWithDefaultImplementationClass,
      Class<T> scalarImplementationClass, TypeReference<List<T>> listOfScalarTypeReference) {
    this.listOfScalarWithDefaulClass = scalarWithDefaultImplementationClass;
    this.listOfScalarImplementationClass = scalarImplementationClass;
    this.listOfScalarTypeReference = listOfScalarTypeReference;
  }
  
  @Override
  public ListOfScalarWithDefault<T> deserialize(JsonParser jp, DeserializationContext ctxt) 
      throws IOException, JsonProcessingException {
    try {
      Constructor<? extends ListOfScalarWithDefault<T>> scalarWithDefaultConstructor = 
          listOfScalarWithDefaulClass.getConstructor(
              List.class, boolean.class);
      
      return scalarWithDefaultConstructor.newInstance(
          jp.getCodec().readValue(jp, listOfScalarTypeReference), false);
    } catch (JsonParseException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new JsonParseException("Error while parsing list of scalar with value for scalar type " 
          + listOfScalarImplementationClass.getSimpleName(), jp.getCurrentLocation(), exception);
    }
  }

}
