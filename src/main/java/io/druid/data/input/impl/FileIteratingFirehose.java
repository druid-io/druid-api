package io.druid.data.input.impl;

import com.google.common.base.Throwables;
import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;
import io.druid.utils.Runnables;
import org.apache.commons.io.LineIterator;

import java.io.IOException;
import java.util.Iterator;

/**
 */
public class FileIteratingFirehose implements Firehose
{
  private final Iterator<LineIterator> lineIterators;
  private final StringInputRowParser parser;

  private LineIterator lineIterator = null;

  public FileIteratingFirehose(
      Iterator<LineIterator> lineIterators,
      StringInputRowParser parser
  )
  {
    this.lineIterators = lineIterators;
    this.parser = parser;
  }

  @Override
  public boolean hasMore()
  {
    try {
      return lineIterators.hasNext() || (lineIterator != null && lineIterator.hasNext());
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public InputRow nextRow()
  {
    try {
      if (lineIterator == null || !lineIterator.hasNext()) {
        // Close old streams, maybe.
        if (lineIterator != null) {
          lineIterator.close();
        }

        lineIterator = lineIterators.next();
      }

      return parser.parse(lineIterator.next());
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Runnable commit()
  {
    return Runnables.getNoopRunnable();
  }

  @Override
  public void close() throws IOException
  {
    if (lineIterator != null) {
      lineIterator.close();
    }
  }
}
