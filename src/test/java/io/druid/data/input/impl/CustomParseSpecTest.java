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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.TestObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class CustomParseSpecTest
{
  private final ObjectMapper jsonMapper = new TestObjectMapper();

  @Test
  public void testSerde() throws IOException
  {
    Map<String, Object> properties = ImmutableMap.of(
        "delimiter", "\u0001",
        "listDelimiter", "\u0002",
        "columns", Arrays.asList("abc")
    );
    CustomParseSpec spec = new CustomParseSpec(
        new TimestampSpec("abc", "iso", null),
        new DimensionsSpec(Arrays.asList("abc"), null, null),
        DelimitedParseSpec.class.getName(),
        properties
    );
    final CustomParseSpec serde = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        CustomParseSpec.class
    );
    Assert.assertEquals("abc", serde.getTimestampSpec().getTimestampColumn());
    Assert.assertEquals("iso", serde.getTimestampSpec().getTimestampFormat());

    Assert.assertEquals(Arrays.asList("abc"), serde.getProperties().get("columns"));
    Assert.assertEquals("\u0001", serde.getProperties().get("delimiter"));
    Assert.assertEquals("\u0002", serde.getProperties().get("listDelimiter"));
    Assert.assertEquals(Arrays.asList("abc"), serde.getDimensionsSpec().getDimensions());
  }
}
