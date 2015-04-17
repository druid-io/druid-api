package io.druid.data.input.impl;

import com.google.common.collect.Lists;
import com.metamx.common.parsers.ParseException;
import org.junit.Test;

import java.util.Arrays;

public class CSVParseSpecTest
{
  @Test(expected = IllegalArgumentException.class)
  public void testColumnMissing() throws Exception
  {
    final ParseSpec spec = new CSVParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        new DimensionsSpec(
            Arrays.asList("a", "b"),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        Arrays.asList("a")
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testComma() throws Exception
  {
    final ParseSpec spec = new CSVParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        new DimensionsSpec(
            Arrays.asList("a,", "b"),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        Arrays.asList("a")
    );
  }
}
