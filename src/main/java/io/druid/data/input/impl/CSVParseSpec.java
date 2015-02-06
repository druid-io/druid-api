package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.metamx.common.parsers.CSVParser;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;

import java.util.List;

/**
 */
public class CSVParseSpec extends ParseSpec
{
  private final String listDelimiter;
  private final List<String> columns;

  @JsonCreator
  public CSVParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("listDelimiter") String listDelimiter,
      @JsonProperty("columns") List<String> columns
  )
  {
    super(timestampSpec, dimensionsSpec);

    this.listDelimiter = listDelimiter;
    Preconditions.checkNotNull(columns, "columns");
    for (String column : columns) {
      Preconditions.checkArgument(!column.contains(","), "Column[%s] has a comma, it cannot", column);
    }

    this.columns = columns;

    verify(dimensionsSpec.getDimensions());
  }

  @JsonProperty
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
    return new CSVParser(Optional.fromNullable(listDelimiter), columns);
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new CSVParseSpec(spec, getDimensionsSpec(), listDelimiter, columns);
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new CSVParseSpec(getTimestampSpec(), spec, listDelimiter, columns);
  }

  public ParseSpec withColumns(List<String> cols)
  {
    return new CSVParseSpec(getTimestampSpec(), getDimensionsSpec(), listDelimiter, cols);
  }
}
