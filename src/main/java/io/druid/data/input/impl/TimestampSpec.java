/*
 * Copyright 2014 Metamarkets Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.common.base.Throwables;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.ParserUtils;
import org.joda.time.DateTime;

import java.util.Map;

/**
 */
public class TimestampSpec
{
  private static final String defaultColumn = "timestamp";
  private static final String defaultFormat = "auto";

  private final String timestampColumn;
  private final String timestampFormat;
  private final Function<String, DateTime> timestampConverter;

  @JsonCreator
  public TimestampSpec(
      @JsonProperty("column") String timestampColumn,
      @JsonProperty("format") String format
  )
  {
    this.timestampColumn = (timestampColumn == null) ? defaultColumn : timestampColumn;
    this.timestampFormat = format == null ? defaultFormat : format;
    this.timestampConverter = ParserUtils.createTimestampParser(timestampFormat);
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

  public DateTime extractTimestamp(Map<String, Object> input)
  {
    final Object o = input.get(timestampColumn);

    return o == null ? null : timestampConverter.apply(o.toString());
  }
}
