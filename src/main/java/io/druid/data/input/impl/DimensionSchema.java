/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;

import java.util.List;

public class DimensionSchema
{
  // This should really use ValueType from main Druid.
  // TODO: get rid of this when druid-api is merged back into the main repo
  public enum ValueType
  {
    FLOAT,
    LONG,
    STRING;

    @JsonValue
    @Override
    public String toString()
    {
      return this.name().toUpperCase();
    }

    @JsonCreator
    public static ValueType fromString(String name)
    {
      return valueOf(name.toUpperCase());
    }
  }

  private final String name;
  private final ValueType type;
  private final boolean isSpatial;
  private final List<String> subdimensions;

  @JsonCreator
  public DimensionSchema(
      @JsonProperty("name") String name,
      @JsonProperty("type") ValueType type,
      @JsonProperty("isSpatial") Boolean isSpatial,
      @JsonProperty("subdimensions") List<String> subdimensions
  )
  {
    Preconditions.checkArgument(name != null, "Dimension name cannot be null.");
    this.name = name;
    this.type = type == null ? ValueType.STRING : type;
    this.isSpatial = isSpatial == null ? false : isSpatial;
    this.subdimensions = subdimensions;
  }

  public DimensionSchema(
      String name
  )
  {
    this(name, null, null, null);
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public ValueType getType()
  {
    return type;
  }

  @JsonProperty("isSpatial")
  public boolean isSpatial()
  {
    return isSpatial;
  }

  @JsonProperty
  public List<String> getSubdimensions()
  {
    return subdimensions;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DimensionSchema that = (DimensionSchema) o;

    if (isSpatial != that.isSpatial) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }
    if (type != that.type) {
      return false;
    }
    return subdimensions != null ? subdimensions.equals(that.subdimensions) : that.subdimensions == null;

  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + type.hashCode();
    result = 31 * result + (isSpatial ? 1 : 0);
    result = 31 * result + (subdimensions != null ? subdimensions.hashCode() : 0);
    return result;
  }
}