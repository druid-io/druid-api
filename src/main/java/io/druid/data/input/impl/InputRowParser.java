package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.InputRow;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = StringInputRowParser.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "string", value = StringInputRowParser.class),
    @JsonSubTypes.Type(name = "map", value = MapInputRowParser.class)
})
public interface InputRowParser<T>
{
  public InputRow parse(T input) throws ParseException;

  public ParseSpec getParseSpec();

  public InputRowParser withParseSpec(ParseSpec parseSpec) throws ParseException;
}
