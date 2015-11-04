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
import java.nio.charset.Charset;

public class InputRowParserSerdeTest
{
  private final ObjectMapper jsonMapper = new TestObjectMapper();

  @Test
  public void testStringInputRowParserSerde() throws Exception
  {
    final StringInputRowParser parser = new StringInputRowParser(
        new JSONParseSpec(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(ImmutableList.of("foo", "bar"), null, null),
            null
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
  public void testStringInputRowParserSerdeMultiCharset() throws Exception
  {
    Charset[] testCharsets = {
        Charsets.US_ASCII, Charsets.ISO_8859_1, Charsets.UTF_8,
        Charsets.UTF_16BE, Charsets.UTF_16LE, Charsets.UTF_16
    };

    for (Charset testCharset : testCharsets) {
      InputRow parsed = testCharsetParseHelper(testCharset);
      Assert.assertEquals(ImmutableList.of("bar", "foo"), parsed.getDimensions());
      Assert.assertEquals(ImmutableList.of("x"), parsed.getDimension("foo"));
      Assert.assertEquals(ImmutableList.of("y"), parsed.getDimension("bar"));
      Assert.assertEquals(new DateTime("3000").getMillis(), parsed.getTimestampFromEpoch());
    }
  }

  @Test
  public void testMapInputRowParserSerde() throws Exception
  {
    final MapInputRowParser parser = new MapInputRowParser(
        new JSONParseSpec(
            new TimestampSpec("timeposix", "posix", null),
            new DimensionsSpec(ImmutableList.of("foo", "bar"), ImmutableList.of("baz"), null),
            null
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
            new TimestampSpec("timemillis", "millis", null),
            new DimensionsSpec(ImmutableList.of("foo", "values"), ImmutableList.of("toobig", "value"), null),
            null
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
    Assert.assertEquals(
        ImmutableList.of("1412705931123", "123.456", "1.23E47", "hello"),
        parsed.getDimension("values")
    );
    Assert.assertEquals(Float.POSITIVE_INFINITY, parsed.getFloatMetric("toobig"));
    Assert.assertEquals(123E64, parsed.getRaw("toobig"));
    Assert.assertEquals(123.456f, parsed.getFloatMetric("value"));
    Assert.assertEquals(123456789000L, parsed.getRaw("long"));
    Assert.assertEquals(1.23456791E11f, parsed.getFloatMetric("long"));
    Assert.assertEquals(1412705931123L, parsed.getTimestampFromEpoch());
  }

  private InputRow testCharsetParseHelper(Charset charset) throws Exception
  {
    final StringInputRowParser parser = new StringInputRowParser(
        new JSONParseSpec(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(ImmutableList.of("foo", "bar"), null, null),
            null
        ),
        charset.name()
    );

    final ByteBufferInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(parser),
        ByteBufferInputRowParser.class
    );

    final InputRow parsed = parser2.parse(
        ByteBuffer.wrap(
            "{\"foo\":\"x\",\"bar\":\"y\",\"qux\":\"z\",\"timestamp\":\"3000\"}".getBytes(charset)
        )
    );

    return parsed;
  }

  @Test
  public void testFlattenParse() throws Exception
  {
    ParseFlattenSpec flattenSpec = new ParseFlattenSpec(true, "$", ImmutableList.of("$met.a"));
    final StringInputRowParser parser = new StringInputRowParser(
        new JSONParseSpec(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(ImmutableList.of("$foo.bar1", "$foo.bar2", "$baz[0]","$baz[1]","$baz[2]", "foo.bar1", "$hey[0].barx"), null, null),
            flattenSpec
        )
    );

    final StringInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(parser),
        StringInputRowParser.class
    );

    final InputRow parsed = parser2.parse(
        "{\"foo\":{\"bar1\":\"aaa\", \"bar2\":\"bbb\"}, \"baz\":[1,2,3], \"timestamp\":\"2999\", \"foo.bar1\":\"Hello world!\", \"hey\":[{\"barx\":\"asdf\"}], \"met\":{\"a\":456}}"
    );
    Assert.assertEquals(ImmutableList.of("$baz[0]","$baz[1]","$baz[2]", "$foo.bar1", "$foo.bar2", "$hey[0].barx", "foo.bar1"), parsed.getDimensions());
    Assert.assertEquals(ImmutableList.of("aaa"), parsed.getDimension("$foo.bar1"));
    Assert.assertEquals(ImmutableList.of("bbb"), parsed.getDimension("$foo.bar2"));
    Assert.assertEquals(ImmutableList.of("1"), parsed.getDimension("$baz[0]"));
    Assert.assertEquals(ImmutableList.of("2"), parsed.getDimension("$baz[1]"));
    Assert.assertEquals(ImmutableList.of("3"), parsed.getDimension("$baz[2]"));
    Assert.assertEquals(ImmutableList.of("Hello world!"), parsed.getDimension("foo.bar1"));
    Assert.assertEquals(ImmutableList.of("asdf"), parsed.getDimension("$hey[0].barx"));
    Assert.assertEquals(ImmutableList.of("456"), parsed.getDimension("$met.a"));
    Assert.assertEquals(new DateTime("2999").getMillis(), parsed.getTimestampFromEpoch());
  }

}
