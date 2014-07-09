package io.druid.timeline.partition;

import io.druid.data.input.InputRow;

public interface ShardSpecLookup
{
  ShardSpec getShardSpec(InputRow row);
}
