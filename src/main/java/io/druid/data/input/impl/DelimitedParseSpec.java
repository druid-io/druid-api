package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.common.parsers.DelimitedParser;
import com.metamx.common.parsers.Parser;

import java.util.List;

/**
 */
public class DelimitedParseSpec extends ParseSpec
{
  private final String delimiter;
  private final List<String> columns;

  @JsonCreator
  public DelimitedParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("delimiter") String delimiter,
      @JsonProperty("columns") List<String> columns
  )
  {
    super(timestampSpec, dimensionsSpec);

    this.columns = columns == null ? Lists.<String>newArrayList() : columns;

    for (String column : this.columns) {
      Preconditions.checkArgument(!column.contains(","), "Column[%s] has a comma, it cannot", column);
    }

    this.delimiter = (delimiter == null) ? DelimitedParser.DEFAULT_DELIMITER : delimiter;
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

  @Override
  public void verify(List<String> usedCols)
  {
    for (String columnName : usedCols) {
      Preconditions.checkArgument(columns.contains(columnName), "column[%s] not in columns.", columnName);
    }
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    Parser<String, Object> retVal = new DelimitedParser(delimiter);
    retVal.setFieldNames(columns);
    return retVal;
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new DelimitedParseSpec(spec, getDimensionsSpec(), delimiter, columns);
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new DelimitedParseSpec(getTimestampSpec(), spec, delimiter, columns);
  }

  public ParseSpec withDelimiter(String delim)
  {
    return new DelimitedParseSpec(getTimestampSpec(), getDimensionsSpec(), delim, columns);
  }

  public ParseSpec withColumns(List<String> cols)
  {
    return new DelimitedParseSpec(getTimestampSpec(), getDimensionsSpec(), delimiter, cols);
  }
}
