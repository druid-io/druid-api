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
    this.timestampColumn = (timestampColumn == null) ? null : timestampColumn.toLowerCase();
    this.timestampFormat = format == null ? defaultFormat : format.toLowerCase();
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
