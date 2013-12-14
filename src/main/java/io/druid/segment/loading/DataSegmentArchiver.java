package io.druid.segment.loading;

import io.druid.timeline.DataSegment;

public interface DataSegmentArchiver
{
  public DataSegment archive(DataSegment segment) throws SegmentLoadingException;
}
