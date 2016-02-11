package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;

import java.util.List;

public class DimensionSchema
{
  // This should really use ValueType from main Druid.
  // TODO: get rid of this when druid-api is merged back into the main repo
  public enum ValueType
  {
    FLOAT,
    LONG,
    STRING;

    @JsonValue
    @Override
    public String toString()
    {
      return this.name().toUpperCase();
    }

    @JsonCreator
    public static ValueType fromString(String name)
    {
      return valueOf(name.toUpperCase());
    }
  }

  private final String name;
  private final ValueType type;
  private final boolean isSpatial;
  private final List<String> subdimensions;

  @JsonCreator
  public DimensionSchema(
      @JsonProperty("name") String name,
      @JsonProperty("type") ValueType type,
      @JsonProperty("isSpatial") Boolean isSpatial,
      @JsonProperty("subdimensions") List<String> subdimensions
  )
  {
    Preconditions.checkArgument(name != null, "Dimension name cannot be null.");
    this.name = name;
    this.type = type == null ? ValueType.STRING : type;
    this.isSpatial = isSpatial == null ? false : isSpatial;
    this.subdimensions = subdimensions;
  }

  public DimensionSchema(
      String name
  )
  {
    this(name, null, null, null);
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public ValueType getType()
  {
    return type;
  }

  @JsonProperty("isSpatial")
  public boolean isSpatial()
  {
    return isSpatial;
  }

  @JsonProperty
  public List<String> getSubdimensions()
  {
    return subdimensions;
  }
}