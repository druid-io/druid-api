/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.data.input.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 */
public class DimensionsSpecSerdeTest
{
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testDimensionsSpecSerde() throws Exception
  {
    String jsonStr = "{\"dimensions\":[\"AAA\", \"BIS\","
                     + "{\"name\":\"CCCP\", \"type\":\"FLOAT\"}, {\"name\":\"DDT\", \"isSpatial\":true},"
                     + "{\"name\":\"EEK\", \"subdimensions\":[\"X\",\"Y\",\"Z\"]}],"
                     + "\"dimensionExclusions\": [\"FOO\", \"HAR\"],"
                     + "\"spatialDimensions\": [{\"dimName\":\"IMPR\", \"dims\":[\"S\",\"P\",\"Q\",\"R\"]}]"
                     + "}";

    DimensionsSpec actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, DimensionsSpec.class)
        ),
        DimensionsSpec.class
    );

    DimensionsSpec expected = new DimensionsSpec(
        Arrays.asList(
            new DimensionSchema("AAA"),
            new DimensionSchema("BIS"),
            new DimensionSchema("CCCP", DimensionSchema.ValueType.FLOAT, false, null),
            new DimensionSchema("DDT", null, true, null),
            new DimensionSchema("EEK", DimensionSchema.ValueType.STRING, false, Arrays.asList("X","Y","Z")),
            new DimensionSchema("IMPR", null, true, Arrays.asList("S","P","Q","R"))
        ),
        Arrays.asList("FOO", "HAR"),
        null
    );

    List<SpatialDimensionSchema> expectedSpatials = Arrays.asList(
        new SpatialDimensionSchema("DDT", null),
        new SpatialDimensionSchema("IMPR",Arrays.asList("S","P","Q","R"))
    );

    Assert.assertEquals(expected, actual);
    Assert.assertEquals(expectedSpatials, actual.getSpatialDimensions());
  }
}
