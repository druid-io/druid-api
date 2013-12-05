package io.druid.segment.loading;

import io.druid.timeline.DataSegment;

public interface DataSegmentMover
{
  public DataSegment move(DataSegment segments) throws SegmentLoadingException;
}
