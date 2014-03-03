package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.parsers.JSONParser;
import com.metamx.common.parsers.Parser;

import java.util.List;

/**
 */
public class JSONParseSpec extends AbstractParseSpec
{
  @JsonCreator
  public JSONParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec
  )
  {
    super(timestampSpec, dimensionsSpec);
  }

  @Override
  public void verify(List<String> usedCols)
  {
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    return new JSONParser();
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new JSONParseSpec(spec, getDimensionsSpec());
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new JSONParseSpec(getTimestampSpec(), spec);
  }
}
