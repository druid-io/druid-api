package io.druid.data.input;

import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;

import java.nio.ByteBuffer;

public interface ByteBufferInputRowParser extends InputRowParser<ByteBuffer>
{
  @Override
  public ByteBufferInputRowParser withParseSpec(ParseSpec parseSpec);
}
