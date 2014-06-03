package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.common.parsers.DelimitedParser;
import com.metamx.common.parsers.Parser;

import java.util.List;

/**
 */
@Deprecated
public class DelimitedDataSpec implements DataSpec
{
  private final String delimiter;
  private final String listDelimiter;
  private final List<String> columns;
  private final List<String> dimensions;
  private final List<SpatialDimensionSchema> spatialDimensions;

  @JsonCreator
  public DelimitedDataSpec(
      @JsonProperty("delimiter") String delimiter,
      @JsonProperty("listDelimiter") String listDelimiter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("spatialDimensions") List<SpatialDimensionSchema> spatialDimensions
  )
  {
    this.delimiter = delimiter;
    this.listDelimiter = listDelimiter;

    Preconditions.checkNotNull(columns);
    for (String column : columns) {
      Preconditions.checkArgument(!column.contains(","), "Column[%s] has a comma, it cannot", column);
    }
    this.columns = columns;
    this.dimensions = dimensions;
    this.spatialDimensions = (spatialDimensions == null)
                             ? Lists.<SpatialDimensionSchema>newArrayList()
                             : spatialDimensions;
  }

  @JsonProperty
  public String getListDelimiter()
  {
    return listDelimiter;
  }

  @JsonProperty("delimiter")
  public String getDelimiter()
  {
    return delimiter;
  }

  @JsonProperty("columns")
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty("dimensions")
  @Override
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty("spatialDimensions")
  @Override
  public List<SpatialDimensionSchema> getSpatialDimensions()
  {
    return spatialDimensions;
  }

  @Override
  public void verify(List<String> usedCols)
  {
    for (String columnName : usedCols) {
      Preconditions.checkArgument(columns.contains(columnName), "column[%s] not in columns.", columnName);
    }
  }

  @Override
  public boolean hasCustomDimensions()
  {
    return !(dimensions == null || dimensions.isEmpty());
  }

  @Override
  public Parser<String, Object> getParser()
  {
    Parser<String, Object> retVal = new DelimitedParser(
        Optional.fromNullable(delimiter),
        Optional.fromNullable(listDelimiter)
    );
    retVal.setFieldNames(columns);
    return retVal;
  }

  @Override
  public ParseSpec toParseSpec(
      TimestampSpec timestampSpec, List<String> dimensionExclusions
  )
  {
    return new DelimitedParseSpec(
        timestampSpec,
        new DimensionsSpec(dimensions, dimensionExclusions, spatialDimensions),
        delimiter,
        listDelimiter,
        columns
    );
  }
}
