package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.InputRow;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Map;

/**
 */
public class StringInputRowParser implements ByteBufferInputRowParser
{
  private final ParseSpec parseSpec;
  private final MapInputRowParser mapParser;
  private final Parser<String, Object> parser;

  private CharBuffer chars = null;

  @JsonCreator
  public StringInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec
  )
  {
    this.parseSpec = parseSpec;
    this.mapParser = new MapInputRowParser(parseSpec);
    this.parser = parseSpec.makeParser();
  }

  @Override
  public InputRow parse(ByteBuffer input)
  {
    return parseMap(buildStringKeyMap(input));
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public StringInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new StringInputRowParser(parseSpec);
  }

  private Map<String, Object> buildStringKeyMap(ByteBuffer input)
  {
    int payloadSize = input.remaining();

    if (chars == null || chars.remaining() < payloadSize) {
      chars = CharBuffer.allocate(payloadSize);
    }

    final CoderResult coderResult = Charsets.UTF_8.newDecoder()
                                                  .onMalformedInput(CodingErrorAction.REPLACE)
                                                  .onUnmappableCharacter(CodingErrorAction.REPLACE)
                                                  .decode(input, chars, true);

    Map<String, Object> theMap;
    if (coderResult.isUnderflow()) {
      chars.flip();
      try {
        theMap = parseString(chars.toString());
      }
      finally {
        chars.clear();
      }
    } else {
      throw new ParseException("Failed with CoderResult[%s]", coderResult);
    }
    return theMap;
  }

  private Map<String, Object> parseString(String inputString)
  {
    return parser.parse(inputString);
  }

  public InputRow parse(String input)
  {
    return parseMap(parseString(input));
  }

  private InputRow parseMap(Map<String, Object> theMap)
  {
    return mapParser.parse(theMap);
  }
}
