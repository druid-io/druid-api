package io.druid.segment.loading;

import io.druid.timeline.DataSegment;

/**
 */
public interface DataSegmentKiller
{
  public void kill(DataSegment segments) throws SegmentLoadingException;
}
