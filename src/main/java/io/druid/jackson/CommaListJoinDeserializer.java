package io.druid.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 */
public class CommaListJoinDeserializer extends StdScalarDeserializer<List<String>>
{
    protected CommaListJoinDeserializer()
  {
    super(List.class);
  }

  @Override
  public List<String> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JsonProcessingException
  {
    return Arrays.asList(jsonParser.getText().split(","));
  }
}
