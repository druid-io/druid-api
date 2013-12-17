package io.druid.segment.loading;

import io.druid.timeline.DataSegment;

import java.util.Map;

public interface DataSegmentMover
{
  public DataSegment move(DataSegment segment, Map<String, Object> targetLoadSpec) throws SegmentLoadingException;
}
