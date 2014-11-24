package io.druid.tasklogs;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import com.google.common.io.InputSupplier;

import java.io.IOException;
import java.io.InputStream;

/**
 * Something that knows how to stream logs for tasks.
 */
public interface TaskLogStreamer
{
  /**
   * Stream log for a task.
   *
   * @param offset If zero, stream the entire log. If positive, attempt to read from this position onwards. If
   *               negative, attempt to read this many bytes from the end of the file (like <tt>tail -n</tt>).
   *
   * @return input supplier for this log, if available from this provider
   */
  public Optional<ByteSource> streamTaskLog(String taskid, long offset) throws IOException;
}
