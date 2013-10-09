package io.druid.segment.loading;

import io.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;

public interface DataSegmentPusher
{
  public DataSegment push(File file, DataSegment segment) throws IOException;
}
