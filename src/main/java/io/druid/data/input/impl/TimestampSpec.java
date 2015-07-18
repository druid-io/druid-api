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
import com.metamx.common.parsers.ParserUtils;
import org.joda.time.DateTime;

import java.util.Map;

/**
 */
public class TimestampSpec
{
  private static final String DEFAULT_COLUMN = "timestamp";
  private static final String DEFAULT_FORMAT = "auto";
  private static final DateTime DEFAULT_MISSING_VALUE = null;

  private final String timestampColumn;
  private final String timestampFormat;
  private final Function<String, DateTime> timestampConverter;
  // this value should never be set for production data
  private final DateTime missingValue;

  @JsonCreator
  public TimestampSpec(
      @JsonProperty("column") String timestampColumn,
      @JsonProperty("format") String format,
      // this value should never be set for production data
      @JsonProperty("missingValue") DateTime missingValue
  )
  {
    this.timestampColumn = (timestampColumn == null) ? DEFAULT_COLUMN : timestampColumn;
    this.timestampFormat = format == null ? DEFAULT_FORMAT : format;
    this.timestampConverter = ParserUtils.createTimestampParser(timestampFormat);
    this.missingValue = missingValue == null
                                       ? DEFAULT_MISSING_VALUE
                                       : missingValue;
  }

  @JsonProperty("column")
  public String getTimestampColumn()
  {
    return timestampColumn;
  }

  @JsonProperty("format")
  public String getTimestampFormat()
  {
    return timestampFormat;
  }

  @JsonProperty("missingValue")
  public DateTime getMissingValue()
  {
    return missingValue;
  }

  public DateTime extractTimestamp(Map<String, Object> input)
  {
    final Object o = input.get(timestampColumn);

    return o == null ? missingValue : timestampConverter.apply(o.toString());
  }
}
