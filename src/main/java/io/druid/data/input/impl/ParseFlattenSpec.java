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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class ParseFlattenSpec
{
  private final boolean enabled;
  private final String nestPrefix;
  private final List<String> metrics;

  @JsonCreator
  public ParseFlattenSpec(
      @JsonProperty("enabled") Boolean enabled,
      @JsonProperty("nestPrefix") String nestPrefix,
      @JsonProperty("metrics") List<String> metrics
  )
  {
    this.enabled = enabled == null ? false : enabled;
    this.metrics = metrics == null ? new ArrayList<String>() : metrics;
    this.nestPrefix = nestPrefix == null ? "$" : nestPrefix;
  }

  @JsonProperty
  public boolean isEnabled()
  {
    return enabled;
  }

  @JsonProperty
  public String getNestPrefix()
  {
    return nestPrefix;
  }

  @JsonProperty
  public List<String> getMetrics()
  {
    return metrics;
  }

}
