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
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.TestObjectMapper;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.InputRow;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.junit.Test;

import java.nio.ByteBuffer;

public class InputRowParserSerdeTest
{
  private final ObjectMapper jsonMapper = new TestObjectMapper();

  @Test
  public void testStringInputRowParserSerde() throws Exception
  {
    final StringInputRowParser parser = new StringInputRowParser(
        new JSONParseSpec(
            new TimestampSpec("timestamp", "iso"),
            new DimensionsSpec(ImmutableList.of("foo", "bar"), null, null)
        )
    );
    final ByteBufferInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(parser),
        ByteBufferInputRowParser.class
    );
    final InputRow parsed = parser2.parse(
        ByteBuffer.wrap(
            "{\"foo\":\"x\",\"bar\":\"y\",\"qux\":\"z\",\"timestamp\":\"2000\"}".getBytes(Charsets.UTF_8)
        )
    );
    Assert.assertEquals(ImmutableList.of("bar", "foo"), parsed.getDimensions());
    Assert.assertEquals(ImmutableList.of("x"), parsed.getDimension("foo"));
    Assert.assertEquals(ImmutableList.of("y"), parsed.getDimension("bar"));
    Assert.assertEquals(new DateTime("2000").getMillis(), parsed.getTimestampFromEpoch());
  }

  @Test
  public void testMapInputRowParserSerde() throws Exception
  {
    final MapInputRowParser parser = new MapInputRowParser(
        new JSONParseSpec(
            new TimestampSpec("timeposix", "posix"),
            new DimensionsSpec(ImmutableList.of("foo", "bar"), ImmutableList.of("baz"), null)
        )
    );
    final MapInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(parser),
        MapInputRowParser.class
    );
    final InputRow parsed = parser2.parse(
        ImmutableMap.<String, Object>of(
            "foo", "x",
            "bar", "y",
            "qux", "z",
            "timeposix", "1"
        )
    );
    Assert.assertEquals(ImmutableList.of("bar", "foo"), parsed.getDimensions());
    Assert.assertEquals(ImmutableList.of("x"), parsed.getDimension("foo"));
    Assert.assertEquals(ImmutableList.of("y"), parsed.getDimension("bar"));
    Assert.assertEquals(1000, parsed.getTimestampFromEpoch());
  }

  @Test
  public void testMapInputRowParserNumbersSerde() throws Exception
  {
    final MapInputRowParser parser = new MapInputRowParser(
        new JSONParseSpec(
            new TimestampSpec("timemillis", "millis"),
            new DimensionsSpec(ImmutableList.of("foo", "values"), ImmutableList.of("toobig", "value"), null)
        )
    );
    final MapInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(parser),
        MapInputRowParser.class
    );
    final InputRow parsed = parser2.parse(
        ImmutableMap.<String, Object>of(
            "timemillis", 1412705931123L,
            "toobig", 123E64,
            "value", 123.456,
            "long", 123456789000L,
            "values", Lists.newArrayList(1412705931123L, 123.456, 123E45, "hello")
        )
    );
    Assert.assertEquals(ImmutableList.of("foo", "values"), parsed.getDimensions());
    Assert.assertEquals(ImmutableList.of(), parsed.getDimension("foo"));
    Assert.assertEquals(ImmutableList.of("1412705931123", "123.456", "1.23E47", "hello"), parsed.getDimension("values"));
    Assert.assertEquals(Float.POSITIVE_INFINITY, parsed.getFloatMetric("toobig"));
    Assert.assertEquals(123E64, parsed.getRaw("toobig"));
    Assert.assertEquals(123.456f, parsed.getFloatMetric("value"));
    Assert.assertEquals(123456789000L, parsed.getRaw("long"));
    Assert.assertEquals(1.23456791E11f, parsed.getFloatMetric("long"));
    Assert.assertEquals(1412705931123L, parsed.getTimestampFromEpoch());
  }

}
