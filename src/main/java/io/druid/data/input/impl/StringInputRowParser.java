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
	private final MapInputRowParser inputRowCreator;
	private final Parser<String, Object> parser;
  private final DataSpec dataSpec;

	private CharBuffer chars = null;

	@JsonCreator
	public StringInputRowParser(
	    @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
	    @JsonProperty("data") DataSpec dataSpec,
	    @JsonProperty("dimensionExclusions") List<String> dimensionExclusions)
	{
    this.dataSpec = dataSpec;
		this.inputRowCreator = new MapInputRowParser(timestampSpec, dataSpec.getDimensions(), dimensionExclusions);
		this.parser = new ToLowerCaseParser(dataSpec.getParser());
	}

	public void addDimensionExclusion(String dimension)
	{
		inputRowCreator.addDimensionExclusion(dimension);
	}

	@Override
	public InputRow parse(ByteBuffer input) throws FormattedException
	{
		return parseMap(buildStringKeyMap(input));
	}

	private Map<String, Object> buildStringKeyMap(ByteBuffer input)
	{
		int payloadSize = input.remaining();

		if (chars == null || chars.remaining() < payloadSize)
		{
			chars = CharBuffer.allocate(payloadSize);
		}

		final CoderResult coderResult = Charsets.UTF_8.newDecoder()
		    .onMalformedInput(CodingErrorAction.REPLACE)
		    .onUnmappableCharacter(CodingErrorAction.REPLACE)
		    .decode(input, chars, true);

		Map<String, Object> theMap;
		if (coderResult.isUnderflow())
		{
			chars.flip();
			try
			{
				theMap = parseString(chars.toString());
			} finally
			{
				chars.clear();
			}
		}
		else
		{
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
		return inputRowCreator.parse(theMap);
	}

  @JsonProperty
  public TimestampSpec getTimestampSpec()
  {
    return inputRowCreator.getTimestampSpec();
  }

  @JsonProperty("data")
  public DataSpec getDataSpec()
  {
    return dataSpec;
  }

  @JsonProperty
  public List<String> getDimensionExclusions()
  {
    return ImmutableList.copyOf(inputRowCreator.getDimensionExclusions());
  }
}
