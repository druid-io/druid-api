package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.impl.InputRowParser;

import java.io.IOException;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface FirehoseFactory<T extends InputRowParser>
{
  /**
   * Initialization method that connects up the fire hose.  If this method returns successfully it should be safe to
   * call hasMore() on the returned Firehose (which might subsequently block).
   * <p/>
   * If this method returns null, then any attempt to call hasMore(), nextRow(), commit() and close() on the return
   * value will throw a surprising NPE.   Throwing IOException on connection failure or runtime exception on
   * invalid configuration is preferred over returning null.
   */
  public Firehose connect(T parser) throws IOException, ParseException;

}
