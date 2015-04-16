package io.druid.data.input.impl;

import com.google.common.collect.ImmutableMap;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.junit.Test;

public class TimestampSpecTest
{
  @Test
  public void testExtractTimestamp() throws Exception
  {
    TimestampSpec spec = new TimestampSpec("TIMEstamp", "yyyy-MM-dd", null);
    Assert.assertEquals(
        new DateTime("2014-03-01"),
        spec.extractTimestamp(ImmutableMap.<String, Object>of("TIMEstamp", "2014-03-01"))
    );
  }

  @Test
  public void testExtractTimestampWithMissingTimestampColumn() throws Exception
  {
    TimestampSpec spec = new TimestampSpec(null, null, new DateTime(0));
    Assert.assertEquals(
        new DateTime("1970-01-01"),
        spec.extractTimestamp(ImmutableMap.<String, Object>of("dim", "foo"))
    );
  }
}
