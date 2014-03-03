package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.metamx.common.parsers.Parser;

import java.util.List;

/**
 */
@Deprecated
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format", defaultImpl = DelimitedDataSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "json", value = JSONDataSpec.class),
    @JsonSubTypes.Type(name = "csv", value = CSVDataSpec.class),
    @JsonSubTypes.Type(name = "tsv", value = DelimitedDataSpec.class)
})
public interface DataSpec
{
  public void verify(List<String> usedCols);

  public boolean hasCustomDimensions();

  public List<String> getDimensions();

  public List<SpatialDimensionSchema> getSpatialDimensions();

  public Parser<String, Object> getParser();

  public ParseSpec toParseSpec(TimestampSpec timestampSpec, List<String> dimensionExclusions);
}
