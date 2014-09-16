package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Sets;
import com.metamx.common.parsers.Parser;

import java.util.List;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format", defaultImpl = DelimitedParseSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "json", value = JSONParseSpec.class),
    @JsonSubTypes.Type(name = "csv", value = CSVParseSpec.class),
    @JsonSubTypes.Type(name = "tsv", value = DelimitedParseSpec.class)
})
public abstract class ParseSpec
{
  private final TimestampSpec timestampSpec;
  private final DimensionsSpec dimensionsSpec;

  protected ParseSpec(TimestampSpec timestampSpec, DimensionsSpec dimensionsSpec)
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
