/*
 * Copyright 2014 Metamarkets Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.data.input.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.TestObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class ParseSpecTest
{
  private final ObjectMapper jsonMapper = new TestObjectMapper();

  @Test
  public void testDelimitedParseSpec() throws IOException
  {
    DelimitedParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec("abc", "iso"),
        new DimensionsSpec(Arrays.asList("abc"), null, null),
        "\u0001",
        "\u0002",
        Arrays.asList("def")
    );
    final DelimitedParseSpec serde = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        DelimitedParseSpec.class
    );
    Assert.assertEquals("abc", serde.getTimestampSpec().getTimestampColumn());
    Assert.assertEquals("iso", serde.getTimestampSpec().getTimestampFormat());

    Assert.assertEquals( Arrays.asList("def"), serde.getColumns());
    Assert.assertEquals("\u0001", serde.getDelimiter());
    Assert.assertEquals("\u0002", serde.getListDelimiter());
    Assert.assertEquals(Arrays.asList("abc"), serde.getDimensionsSpec().getDimensions());
  }
}
