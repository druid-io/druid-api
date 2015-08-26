/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import java.nio.charset.Charset;
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
  private final String encoding;

  private CharBuffer chars = null;

  @JsonCreator
  public StringInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("encoding") String encoding
  )
  {
    this.parseSpec = parseSpec;
    this.mapParser = new MapInputRowParser(parseSpec);
    this.parser = parseSpec.makeParser();
    if(encoding != null) {
      this.encoding = encoding;
    } else {
      this.encoding = Charsets.UTF_8.name();
    }
  }

  public StringInputRowParser(ParseSpec parseSpec)
  {
    this.parseSpec = parseSpec;
    this.mapParser = new MapInputRowParser(parseSpec);
    this.parser = parseSpec.makeParser();
    this.encoding = Charsets.UTF_8.name();
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

  @JsonProperty
  public String getEncoding()
  {
    return encoding;
  }

  @Override
  public StringInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new StringInputRowParser(parseSpec, encoding);
  }

  private Map<String, Object> buildStringKeyMap(ByteBuffer input)
  {
    int payloadSize = input.remaining();

    if (chars == null || chars.remaining() < payloadSize) {
      chars = CharBuffer.allocate(payloadSize);
    }

    Charset charset = Charset.forName(encoding);
    final CoderResult coderResult = charset.newDecoder()
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
