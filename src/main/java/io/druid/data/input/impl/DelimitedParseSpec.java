package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.common.parsers.DelimitedParser;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;

import java.util.List;

/**
 */
public class DelimitedParseSpec extends ParseSpec
{
  private final String delimiter;
  private final String listDelimiter;
  private final List<String> columns;

  @JsonCreator
  public DelimitedParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("delimiter") String delimiter,
      @JsonProperty("listDelimiter") String listDelimiter,
      @JsonProperty("columns") List<String> columns
  )
  {
    super(timestampSpec, dimensionsSpec);

    this.delimiter = delimiter;
    this.listDelimiter = listDelimiter;

    this.columns = columns == null ? dimensionsSpec.getDimensions() : columns;
    for (String column : this.columns) {
      Preconditions.checkArgument(!column.contains(","), "Column[%s] has a comma, it cannot", column);
    }

    verify(dimensionsSpec.getDimensions());
  }

  @JsonProperty("delimiter")
  public String getDelimiter()
  {
    return delimiter;
  }

  @JsonProperty("listDelimiter")
  public String getListDelimiter()
  {
    return listDelimiter;
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
    Parser<String, Object> retVal = new DelimitedParser(
        Optional.fromNullable(delimiter),
        Optional.fromNullable(listDelimiter)
    );
    retVal.setFieldNames(columns);
    return retVal;
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new DelimitedParseSpec(spec, getDimensionsSpec(), delimiter, listDelimiter, columns);
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new DelimitedParseSpec(getTimestampSpec(), spec, delimiter, listDelimiter, columns);
  }

  public ParseSpec withDelimiter(String delim)
  {
    return new DelimitedParseSpec(getTimestampSpec(), getDimensionsSpec(), delim, listDelimiter, columns);
  }

  public ParseSpec withListDelimiter(String delim)
  {
    return new DelimitedParseSpec(getTimestampSpec(), getDimensionsSpec(), delimiter, delim, columns);
  }

  public ParseSpec withColumns(List<String> cols)
  {
    return new DelimitedParseSpec(getTimestampSpec(), getDimensionsSpec(), delimiter, listDelimiter, cols);
  }
}
