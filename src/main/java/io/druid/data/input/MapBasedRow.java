package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 */
public class MapBasedRow implements Row
{
  private static final Logger log = new Logger(MapBasedRow.class);
  private static final Function<Object, String> TO_STRING_INCLUDING_NULL = new ToStringFunction();
  private static final Pattern LONG_PAT = Pattern.compile("[-|+]?\\d+");
  private final DateTime timestamp;
  private final Map<String, Object> event;

  @JsonCreator
  public MapBasedRow(
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("event") Map<String, Object> event
  )
  {
    this.timestamp = timestamp;
    this.event = event;
  }

  public MapBasedRow(
      long timestamp,
      Map<String, Object> event
  )
  {
    this(new DateTime(timestamp), event);
  }

  @Override
  public long getTimestampFromEpoch()
  {
    return timestamp.getMillis();
  }

  @JsonProperty
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty
  public Map<String, Object> getEvent()
  {
    return event;
  }

  @Override
  public List<String> getDimension(String dimension)
  {
    final Object dimValue = event.get(dimension);

    if (dimValue == null) {
      return Collections.emptyList();
    } else if (dimValue instanceof List) {
      return Lists.transform((List<Object>) dimValue, TO_STRING_INCLUDING_NULL);
    } else {
      return Collections.singletonList(String.valueOf(dimValue));
    }
  }

  @Override
  public Object getRaw(String dimension)
  {
    return event.get(dimension);
  }

  @Override
  public float getFloatMetric(String metric)
  {
    Object metricValue = event.get(metric);

    if (metricValue == null) {
      return 0.0f;
    }

    if (metricValue instanceof Number) {
      return ((Number) metricValue).floatValue();
    } else if (metricValue instanceof String) {
      try {
        return Float.valueOf(((String) metricValue).replace(",", ""));
      }
      catch (Exception e) {
        throw new ParseException(e, "Unable to parse metrics[%s], value[%s]", metric, metricValue);
      }
    } else {
      throw new ParseException("Unknown type[%s]", metricValue.getClass());
    }
  }

  @Override
  public long getLongMetric(String metric)
  {
    Object metricValue = event.get(metric);

    if (metricValue == null) {
      return 0L;
    }

    if (metricValue instanceof Number) {
      return ((Number) metricValue).longValue();
    } else if (metricValue instanceof String) {
      try {
        String s = ((String) metricValue).replace(",", "");
        return LONG_PAT.matcher(s).matches() ? Long.valueOf(s) : Double.valueOf(s).longValue();
      }
      catch (Exception e) {
        throw new ParseException(e, "Unable to parse metrics[%s], value[%s]", metric, metricValue);
      }
    } else {
      throw new ParseException("Unknown type[%s]", metricValue.getClass());
    }
  }

  @Override
  public String toString()
  {
    return "MapBasedRow{" +
           "timestamp=" + timestamp +
           ", event=" + event +
           '}';
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

    MapBasedRow that = (MapBasedRow) o;

    if (!event.equals(that.event)) {
      return false;
    }
    return timestamp.equals(that.timestamp);

  }

  @Override
  public int hashCode()
  {
    int result = timestamp.hashCode();
    result = 31 * result + event.hashCode();
    return result;
  }

  @Override
  public int compareTo(Row o)
  {
    return timestamp.compareTo(o.getTimestamp());
  }

  // guava's toString function checks on non-null objects, so do not use it
  private static final class ToStringFunction implements Function<Object, String>
  {
    @Override
    public String apply(final Object input)
    {
      return String.valueOf(input);
    }
  }
}
