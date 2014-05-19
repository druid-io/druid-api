package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.metamx.common.exception.FormattedException;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.ToLowerCaseParser;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.InputRow;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.List;
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
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      // Backwards compatible
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("data") final DataSpec dataSpec,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("dimensionExclusions") List<String> dimensionExclusions
  )
  {
    if (parseSpec == null) {
      if (dataSpec == null) {
        this.parseSpec = new JSONParseSpec(
            timestampSpec,
            new DimensionsSpec(
                dimensions,
                dimensionExclusions,
                ImmutableList.<SpatialDimensionSchema>of()
            )
        );
      } else {
        this.parseSpec = dataSpec.toParseSpec(timestampSpec, dimensionExclusions);
      }
      this.mapParser = new MapInputRowParser(this.parseSpec, null, null, null, null);
      this.parser = new ToLowerCaseParser(this.parseSpec.makeParser());
    } else {
      this.parseSpec = parseSpec;
      this.mapParser = new MapInputRowParser(parseSpec, null, null, null, null);
      this.parser = new ToLowerCaseParser(parseSpec.makeParser());
    }
  }

  @Override
  public InputRow parse(ByteBuffer input) throws FormattedException
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
    return new StringInputRowParser(parseSpec, null, null, null, null);
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
      throw new FormattedException.Builder()
          .withErrorCode(FormattedException.ErrorCode.UNPARSABLE_ROW)
          .withMessage(String.format("Failed with CoderResult[%s]", coderResult))
          .build();
    }
    return theMap;
  }

  private Map<String, Object> parseString(String inputString)
  {
    return parser.parse(inputString);
  }

  public InputRow parse(String input) throws FormattedException
  {
    return parseMap(parseString(input));
  }

  private InputRow parseMap(Map<String, Object> theMap)
  {
    return mapParser.parse(theMap);
  }
}
