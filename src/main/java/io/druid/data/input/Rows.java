package io.druid.data.input;

import com.google.common.collect.Maps;
import com.metamx.common.ISE;

import java.util.List;
import java.util.TreeMap;

/**
 */
public class Rows
{
  public static InputRow toCaseInsensitiveInputRow(final Row row, final List<String> dimensions)
  {
    if (row instanceof MapBasedRow) {
      MapBasedRow mapBasedRow = (MapBasedRow) row;

      TreeMap<String, Object> caseInsensitiveMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
      caseInsensitiveMap.putAll(mapBasedRow.getEvent());
      return new MapBasedInputRow(
          mapBasedRow.getTimestampFromEpoch(),
          dimensions,
          caseInsensitiveMap
      );
    }
    throw new ISE("Can only convert MapBasedRow objects because we are ghetto like that.");
  }
}
