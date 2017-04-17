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
import com.google.common.collect.ImmutableMap;
import com.metamx.common.parsers.Parser;

import java.lang.reflect.Constructor;
import java.util.Map;

/**
 */
public class CustomParseSpec extends ParseSpec
{

  private final String parserClass;
  private final Map<String, Object> properties;

  private final Constructor constructor;

  @JsonCreator
  public CustomParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("parserClass") String parserClass,
      @JsonProperty("properties") Map<String, Object> properties
  )
  {
    super(timestampSpec, dimensionsSpec);

    this.parserClass = parserClass;
    this.properties = properties == null ? ImmutableMap.<String, Object>of() : properties;
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    if (loader == null) {
      loader = ParseSpec.class.getClassLoader();
    }
    try {
      constructor = Class.forName(parserClass, true, loader)
                         .getConstructor(TimestampSpec.class, DimensionsSpec.class, Map.class);
    }
    catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to find proper constructor of parser `" + parserClass + "` by exception " + e,
          e
      );
    }
  }

  @JsonProperty("parserClass")
  public String getParserClass()
  {
    return parserClass;
  }

  @JsonProperty("properties")
  public Map<String, Object> getProperties()
  {
    return properties;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Parser<String, Object> makeParser()
  {
    try {
      return (Parser<String, Object>) constructor.newInstance(getTimestampSpec(), getDimensionsSpec(), getProperties());
    }
    catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new CustomParseSpec(spec, getDimensionsSpec(), getParserClass(), getProperties());
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new CustomParseSpec(getTimestampSpec(), spec, getParserClass(), getProperties());
  }
}
