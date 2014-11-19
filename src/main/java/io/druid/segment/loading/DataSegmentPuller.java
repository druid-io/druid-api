package io.druid.segment.loading;

import io.druid.timeline.DataSegment;

import java.io.File;

/**
 */
public interface DataSegmentPuller
{
  /**
   * Pull down segment files for the given DataSegment and put them in the given directory.
   *
   * @param segment The segment to pull down files for
   * @param dir The directory to store the files in
   * @throws SegmentLoadingException if there are any errors
   */
  public void getSegmentFiles(DataSegment segment, File dir) throws SegmentLoadingException;
}
