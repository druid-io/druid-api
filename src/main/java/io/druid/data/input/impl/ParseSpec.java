package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
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
public interface ParseSpec
{
  public TimestampSpec getTimestampSpec();

  public DimensionsSpec getDimensionsSpec();

  public void verify(List<String> usedCols);

  public Parser<String, Object> makeParser();

  public ParseSpec withTimestampSpec(TimestampSpec spec);

  public ParseSpec withDimensionsSpec(DimensionsSpec spec);
}
