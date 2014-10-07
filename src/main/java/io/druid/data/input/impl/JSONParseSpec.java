package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.metamx.common.IAE;
import com.metamx.common.parsers.JSONParser;
import com.metamx.common.parsers.Parser;

import java.util.List;

/**
 */
public class JSONParseSpec extends ParseSpec
{
  public static final String JSON = "application/json";
  public static final String SMILE = "application/smile";
  public static final String X_JACKSON_SMILE = "application/x-jackson-smile";

  private final ObjectMapper objectMapper;
  private final String contentType;

  @JsonCreator
  public JSONParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("contentType") String contentType
  )
  {
    super(timestampSpec, dimensionsSpec);
    this.contentType = contentType == null ? JSON : contentType;
    switch(this.contentType) {
      case JSON:
        this.objectMapper = new ObjectMapper();
        break;
      case SMILE:
      case X_JACKSON_SMILE:
        this.objectMapper = new ObjectMapper(new SmileFactory());
        break;
      default:
        throw new IAE("Unknown content type[%s]", contentType);
    }

  }

  @Override
  public void verify(List<String> usedCols)
  {
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    return new JSONParser(objectMapper, null, getDimensionsSpec().getDimensionExclusions());
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new JSONParseSpec(spec, getDimensionsSpec(), contentType);
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new JSONParseSpec(getTimestampSpec(), spec, contentType);
  }
}
