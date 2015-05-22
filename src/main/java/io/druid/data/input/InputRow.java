package io.druid.data.input;

import java.io.Serializable;
import java.util.List;

/**
 * An InputRow is the interface definition of an event being input into the data ingestion layer.
 *
 * An InputRow is a Row with a self-describing list of the dimensions available.  This list is used to
 * implement "schema-less" data ingestion that allows the system to add new dimensions as they appear.
 *
 * Note, Druid is a case-insensitive system for parts of schema (column names), this has direct implications
 * for the implementation of InputRows and Rows.  The case-insensitiveness is implemented by lowercasing all
 * schema elements before looking them up, this means that calls to getDimension() and getFloatMetric() will
 * have all lowercase column names passed in no matter what is returned from getDimensions or passed in as the
 * fieldName of an AggregatorFactory.  Implementations of InputRow and Row should expect to get values back
 * in all lowercase form (i.e. they should either have already turned everything into lowercase or they
 * should operate in a case-insensitive manner).
 */
public interface InputRow extends Row ,Serializable
{
  /**
   * Returns the dimensions that exist in this row.
   *
   * @return the dimensions that exist in this row.
   */
  public List<String> getDimensions();
}
