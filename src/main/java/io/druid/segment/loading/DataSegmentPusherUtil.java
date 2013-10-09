package io.druid.segment.loading;

import com.google.common.base.Joiner;
import io.druid.timeline.DataSegment;
import org.joda.time.format.ISODateTimeFormat;

/**
 */
public class DataSegmentPusherUtil
{
	private static final Joiner JOINER = Joiner.on("/").skipNulls();

	public static String getStorageDir(DataSegment segment)
	{
		return JOINER.join(
		    segment.getDataSource(),
		    String.format(
		        "%s_%s",
		        segment.getInterval().getStart(),
            segment.getInterval().getEnd()
        ),
        segment.getVersion(),
        segment.getShardSpec().getPartitionNum()
    );
  }

	/**
	 * Due to https://issues.apache.org/jira/browse/HDFS-13 ":" are not allowed in
	 * path names. So we format paths differently for HDFS.
	 */
	public static String getHdfsStorageDir(DataSegment segment)
	{
		return JOINER.join(
		    segment.getDataSource(),
		    String.format(
		        "%s_%s",
		        segment.getInterval().getStart().toString(ISODateTimeFormat.basicDateTime()),
		        segment.getInterval().getEnd().toString(ISODateTimeFormat.basicDateTime())
		        ),
		    segment.getVersion().replaceAll(":", "_"),
		    segment.getShardSpec().getPartitionNum()
		    );
	}
}
