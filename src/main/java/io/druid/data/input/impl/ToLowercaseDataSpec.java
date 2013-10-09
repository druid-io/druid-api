package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonValue;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.ToLowerCaseParser;

import java.util.List;

/**
 */
public class ToLowercaseDataSpec implements DataSpec
{
  private final DataSpec delegate;

  public ToLowercaseDataSpec(
      DataSpec delegate
  )
  {
    this.delegate = delegate;
  }

  @Override
  public void verify(List<String> usedCols)
  {
    delegate.verify(usedCols);
  }

  @Override
  public boolean hasCustomDimensions()
  {
    return delegate.hasCustomDimensions();
  }

  @Override
  public List<String> getDimensions()
  {
    return delegate.getDimensions();
  }

  @Override
  public List<SpatialDimensionSchema> getSpatialDimensions()
  {
    return delegate.getSpatialDimensions();
  }

  @Override
  public Parser<String, Object> getParser()
  {
    return new ToLowerCaseParser(delegate.getParser());
  }

  @JsonValue
  public DataSpec getDelegate()
  {
    return delegate;
  }
}
