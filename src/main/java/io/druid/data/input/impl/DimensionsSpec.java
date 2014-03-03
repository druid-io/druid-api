package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 */
public class DimensionsSpec
{
  private final List<String> dimensions;
  private final Set<String> dimensionExclusions;
  private final List<SpatialDimensionSchema> spatialDimensions;

  @JsonCreator
  public DimensionsSpec(
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("dimensionExclusions") List<String> dimensionExclusions,
      @JsonProperty("spatialDimensions") List<SpatialDimensionSchema> spatialDimensions
  )
  {
    this.dimensions = dimensions;
    this.dimensionExclusions = (dimensionExclusions == null)
                               ? Sets.<String>newHashSet()
                               : Sets.newHashSet(dimensionExclusions);
    this.spatialDimensions = (spatialDimensions == null)
                             ? Lists.<SpatialDimensionSchema>newArrayList()
                             : spatialDimensions;
  }

  @JsonProperty
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public Set<String> getDimensionExclusions()
  {
    return dimensionExclusions;
  }

  @JsonProperty
  public List<SpatialDimensionSchema> getSpatialDimensions()
  {
    return spatialDimensions;
  }

  public boolean hasCustomDimensions()
  {
    return !(dimensions == null || dimensions.isEmpty());
  }

  public DimensionsSpec withDimensions(List<String> dims)
  {
    return new DimensionsSpec(dims, ImmutableList.copyOf(dimensionExclusions), spatialDimensions);
  }

  public DimensionsSpec withDimensionExclusions(Set<String> dimExs)
  {
    return new DimensionsSpec(dimensions, ImmutableList.copyOf(dimExs), spatialDimensions);
  }

  public DimensionsSpec withSpatialDimensions(List<SpatialDimensionSchema> spatials)
  {
    return new DimensionsSpec(dimensions, ImmutableList.copyOf(dimensionExclusions), spatials);
  }
}
