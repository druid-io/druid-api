package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.parsers.Parser;

import java.util.List;

/**
 */
public abstract class AbstractParseSpec implements ParseSpec
{
  private final TimestampSpec timestampSpec;
  private final DimensionsSpec dimensionsSpec;

  protected AbstractParseSpec(TimestampSpec timestampSpec, DimensionsSpec dimensionsSpec)
  {
    this.timestampSpec = timestampSpec;
    this.dimensionsSpec = dimensionsSpec;
  }

  @JsonProperty
  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  @JsonProperty
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  public void verify(List<String> usedCols)
  {
    // do nothing
  }

  public Parser<String, Object> makeParser()
  {
    return null;
  }

  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    throw new UnsupportedOperationException();
  }

  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    throw new UnsupportedOperationException();
  }
}
