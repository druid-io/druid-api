package io.druid.data.input;

import java.util.List;

/**
 * An InputRow is the interface definition of an event being input into the data ingestion layer.
 *
 * An InputRow is a Row with a self-describing list of the dimensions available.  This list is used to
 * implement "schema-less" data ingestion that allows the system to add new dimensions as they appear.
 *
 */
public interface
    InputRow extends Row
{
  /**
   * Returns the dimensions that exist in this row.
   *
   * @return the dimensions that exist in this row.
   */
  public List<String> getDimensions();
}
