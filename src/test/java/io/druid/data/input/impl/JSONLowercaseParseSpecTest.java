package io.druid.data.input.impl;

import com.google.common.collect.Lists;
import com.metamx.common.parsers.JSONToLowerParser;
import com.metamx.common.parsers.Parser;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class JSONLowercaseParseSpecTest
{
  @Test
  public void testLowercasing() throws Exception
  {
    JSONLowercaseParseSpec spec = new JSONLowercaseParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto"
        ),
        new DimensionsSpec(
            Arrays.asList("A", "B"),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        )
    );
    Parser parser = spec.makeParser();
    Map<String, Object> event = parser.parse("{\"timestamp\":\"2015-01-01\",\"A\":\"foo\"}");
    Assert.assertEquals("foo", event.get("a"));
  }
}
