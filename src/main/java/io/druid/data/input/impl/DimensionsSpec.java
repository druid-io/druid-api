/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metamx.common.parsers.ParserUtils;

import java.util.Collections;
import java.util.List;
import java.util.Set;

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
    this.dimensions = dimensions == null
                      ? Lists.<String>newArrayList()
                      : Lists.newArrayList(dimensions);

    this.dimensionExclusions = (dimensionExclusions == null)
                               ? Sets.<String>newHashSet()
                               : Sets.newHashSet(dimensionExclusions);

    this.spatialDimensions = (spatialDimensions == null)
                             ? Lists.<SpatialDimensionSchema>newArrayList()
                             : spatialDimensions;

    verify();
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
    return new DimensionsSpec(
        dimensions,
        ImmutableList.copyOf(Sets.union(dimensionExclusions, dimExs)),
        spatialDimensions
    );
  }

  public DimensionsSpec withSpatialDimensions(List<SpatialDimensionSchema> spatials)
  {
    return new DimensionsSpec(dimensions, ImmutableList.copyOf(dimensionExclusions), spatials);
  }

  private void verify()
  {
    Preconditions.checkArgument(
        Sets.intersection(this.dimensionExclusions, Sets.newHashSet(this.dimensions)).isEmpty(),
        "dimensions and dimensions exclusions cannot overlap"
    );

    ParserUtils.validateFields(dimensions);
    ParserUtils.validateFields(dimensionExclusions);
    ParserUtils.validateFields(
        Iterables.transform(
            spatialDimensions,
            new Function<SpatialDimensionSchema, String>()
            {
              @Override
              public String apply(SpatialDimensionSchema input)
              {
                return input.getDimName();
              }
            }
        )
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DimensionsSpec that = (DimensionsSpec) o;

    if (!dimensions.equals(that.dimensions)) {
      return false;
    }
    if (!dimensionExclusions.equals(that.dimensionExclusions)) {
      return false;
    }
    return spatialDimensions.equals(that.spatialDimensions);

  }

  @Override
  public int hashCode()
  {
    int result = dimensions.hashCode();
    result = 31 * result + dimensionExclusions.hashCode();
    result = 31 * result + spatialDimensions.hashCode();
    return result;
  }
}
