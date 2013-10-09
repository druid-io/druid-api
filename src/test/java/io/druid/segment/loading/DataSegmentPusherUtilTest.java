package io.druid.segment.loading;

import com.google.common.collect.ImmutableMap;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class DataSegmentPusherUtilTest
{
    @Test
    public void shouldNotHaveColonsInHdfsStorageDir() throws Exception {

        Interval interval = new Interval("2011-10-01/2011-10-02");
        ImmutableMap<String, Object> loadSpec = ImmutableMap.<String, Object>of("something", "or_other");

        DataSegment segment = new DataSegment(
                "something",
                interval,
                "brand:new:version",
                loadSpec,
                Arrays.asList("dim1", "dim2"),
                Arrays.asList("met1", "met2"),
                new NoneShardSpec(),
                null,
                1
        );

        String storageDir = DataSegmentPusherUtil.getHdfsStorageDir(segment);
        Assert.assertEquals("something/20111001T000000.000Z_20111002T000000.000Z/brand_new_version/0", storageDir);

    }
}
