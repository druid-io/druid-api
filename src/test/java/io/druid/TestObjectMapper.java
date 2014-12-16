package io.druid;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import org.joda.time.Interval;

import java.io.IOException;

/**
 */
public class TestObjectMapper extends ObjectMapper
{
  public TestObjectMapper()
  {
    configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    configure(MapperFeature.AUTO_DETECT_GETTERS, false);
    configure(MapperFeature.AUTO_DETECT_FIELDS, false);
    configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
    configure(MapperFeature.AUTO_DETECT_SETTERS, false);
    configure(SerializationFeature.INDENT_OUTPUT, false);
    registerModule(new TestModule());
  }

  public static class TestModule extends SimpleModule
  {
    TestModule()
    {
      addSerializer(Interval.class, ToStringSerializer.instance);
      addDeserializer(
          Interval.class, new StdDeserializer<Interval>(Interval.class)
          {
            @Override
            public Interval deserialize(
                JsonParser jsonParser, DeserializationContext deserializationContext
            ) throws IOException, JsonProcessingException
            {
              return new Interval(jsonParser.getText());
            }
          }
      );
    }
  }
}
