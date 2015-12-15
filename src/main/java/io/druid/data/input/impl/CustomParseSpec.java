package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.parsers.Parser;

import java.lang.reflect.Constructor;
import java.util.Map;

/**
 */
public class CustomParseSpec extends ParseSpec
{

  private final String parserClass;
  private final Map<String, Object> properties;

  private final Constructor constructor;

  @JsonCreator
  public CustomParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("parserClass") String parserClass,
      @JsonProperty("properties") Map<String, Object> properties
  )
  {
    super(timestampSpec, dimensionsSpec);

    this.parserClass = parserClass;
    this.properties = properties == null ? ImmutableMap.<String, Object>of() : properties;
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    if (loader == null) {
      loader = ParseSpec.class.getClassLoader();
    }
    try {
      constructor = Class.forName(parserClass, true, loader)
                         .getConstructor(TimestampSpec.class, DimensionsSpec.class, Map.class);
    }
    catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  @JsonProperty("parserClass")
  public String getParserClass()
  {
    return parserClass;
  }

  @JsonProperty("properties")
  public Map<String, Object> getProperties()
  {
    return properties;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Parser<String, Object> makeParser()
  {
    try {
      return (Parser<String, Object>) constructor.newInstance(getTimestampSpec(), getDimensionsSpec(), getProperties());
    }
    catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new CustomParseSpec(spec, getDimensionsSpec(), getParserClass(), getProperties());
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new CustomParseSpec(getTimestampSpec(), spec, getParserClass(), getProperties());
  }
}
