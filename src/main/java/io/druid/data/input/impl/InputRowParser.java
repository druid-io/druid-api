package io.druid.data.input.impl;

import com.metamx.common.exception.FormattedException;
import io.druid.data.input.InputRow;

public interface InputRowParser<T>
{
  public InputRow parse(T input) throws FormattedException;
  public void addDimensionExclusion(String dimension);
}
