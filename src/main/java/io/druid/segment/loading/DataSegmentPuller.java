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

  /**
   * Returns the last modified time of the given segment.
   *
   * Note, this is not actually used at this point and doesn't need to actually be implemented.  It's just still here
   * to not break compatibility.
   *
   * @param segment The segment to check the last modified time for
   * @return the last modified time in millis from the epoch
   * @throws SegmentLoadingException if there are any errors
   */
  @Deprecated
  public long getLastModified(DataSegment segment) throws SegmentLoadingException;
}
