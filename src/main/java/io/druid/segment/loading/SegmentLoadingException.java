package io.druid.segment.loading;

/**
 */
public class SegmentLoadingException extends Exception
{
  public SegmentLoadingException(
      String formatString,
      Object... objs
  )
  {
    super(String.format(formatString, objs));
  }

  public SegmentLoadingException(
      Throwable cause,
      String formatString,
      Object... objs
  )
  {
    super(String.format(formatString, objs), cause);
  }
}
