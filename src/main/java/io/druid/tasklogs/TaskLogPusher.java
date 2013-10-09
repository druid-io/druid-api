package io.druid.tasklogs;

import java.io.File;
import java.io.IOException;

/**
 * Something that knows how to persist local task logs to some form of long-term storage.
 */
public interface TaskLogPusher
{
  public void pushTaskLog(String taskid, File logFile) throws IOException;
}
