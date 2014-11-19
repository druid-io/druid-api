package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 */
public class SpatialDimensionSchema
{
  private final String dimName;
  private final List<String> dims;

  @JsonCreator
  public SpatialDimensionSchema(
      @JsonProperty("dimName") String dimName,
      @JsonProperty("dims") List<String> dims
  )
  {
    this.dimName = dimName;
    this.dims = dims;
  }

  @JsonProperty
  public String getDimName()
  {
    return dimName;
  }

  @JsonProperty
  public List<String> getDims()
  {
    return dims;
  }
}
