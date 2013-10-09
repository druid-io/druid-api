package io.druid.jackson;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;
import com.google.common.base.Joiner;

import java.io.IOException;
import java.util.List;

/**
 */
public class CommaListJoinSerializer extends StdScalarSerializer<List<String>>
{
  private static final Joiner joiner = Joiner.on(",");

  protected CommaListJoinSerializer()
  {
    super(List.class, true);
  }

  @Override
  public void serialize(List<String> value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException, JsonGenerationException
  {
    jgen.writeString(joiner.join(value));
  }
}
