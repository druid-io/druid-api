package io.druid.data.input.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.druid.TestObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class DelimitedParseSpecTest
{
  private final ObjectMapper jsonMapper = new TestObjectMapper();

  @Test
  public void testSerde() throws IOException
  {
    DelimitedParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec("abc", "iso"),
        new DimensionsSpec(Arrays.asList("abc"), null, null),
        "\u0001",
        "\u0002",
        Arrays.asList("abc")
    );
    final DelimitedParseSpec serde = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        DelimitedParseSpec.class
    );
    Assert.assertEquals("abc", serde.getTimestampSpec().getTimestampColumn());
    Assert.assertEquals("iso", serde.getTimestampSpec().getTimestampFormat());

    Assert.assertEquals(Arrays.asList("abc"), serde.getColumns());
    Assert.assertEquals("\u0001", serde.getDelimiter());
    Assert.assertEquals("\u0002", serde.getListDelimiter());
    Assert.assertEquals(Arrays.asList("abc"), serde.getDimensionsSpec().getDimensions());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testColumnMissing() throws Exception
  {
    final ParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto"
        ),
        new DimensionsSpec(
            Arrays.asList("a", "b"),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        " ",
        Arrays.asList("a")
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testComma() throws Exception
  {
    final ParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto"
        ),
        new DimensionsSpec(
            Arrays.asList("a,", "b"),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        null,
        Arrays.asList("a")
    );
  }

  @Test
  public void testDefaultColumnList(){
    final DelimitedParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto"
        ),
        new DimensionsSpec(
            Arrays.asList("a", "b"),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        null,
        // pass null columns, use default
        null
    );
    Assert.assertEquals(spec.getColumns(), spec.getDimensionsSpec().getDimensions());
  }
}
