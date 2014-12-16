/*
 * Copyright 2014 Metamarkets Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.data.input;

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
public interface InputRow extends Row
{
  /**
   * Returns the dimensions that exist in this row.
   *
   * @return the dimensions that exist in this row.
   */
  public List<String> getDimensions();
}
