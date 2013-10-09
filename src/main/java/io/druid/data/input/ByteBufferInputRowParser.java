package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.StringInputRowParser;

import java.nio.ByteBuffer;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = StringInputRowParser.class)
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "string", value = StringInputRowParser.class)
})
public interface ByteBufferInputRowParser extends InputRowParser<ByteBuffer>
{
}
