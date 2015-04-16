package io.druid.data.input.impl;

import com.google.common.collect.Lists;
import com.metamx.common.parsers.ParseException;
import org.junit.Test;

import java.util.Arrays;

public class ParseSpecTest
{
  @Test(expected = ParseException.class)
  public void testDuplicateNames() throws Exception
  {
    final ParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        new DimensionsSpec(
            Arrays.asList("a", "b", "a"),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        " ",
        Arrays.asList("a", "b")
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDimAndDimExcluOverlap() throws Exception
  {
    final ParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        new DimensionsSpec(
            Arrays.asList("a", "B"),
            Lists.newArrayList("B"),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        null,
        Arrays.asList("a", "B")
    );
  }

  @Test
  public void testDimExclusionDuplicate() throws Exception
  {
    final ParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        new DimensionsSpec(
            Arrays.asList("a"),
            Lists.newArrayList("B", "B"),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        null,
        Arrays.asList("a", "B")
    );
  }
}
