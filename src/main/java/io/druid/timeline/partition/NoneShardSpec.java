package io.druid.timeline.partition;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.metamx.common.ISE;
import io.druid.data.input.InputRow;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 */
public class NoneShardSpec implements ShardSpec
{
  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return new SingleElementPartitionChunk<T>(obj);
  }

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    return true;
  }

  @Override
  public int getPartitionNum()
  {
    return 0;
  }

  @Override
  public ShardSpecLookup getLookup(final List<ShardSpec> shardSpecs)
  {

    return new ShardSpecLookup()
    {
      @Override
      public ShardSpec getShardSpec(long timestamp, InputRow row)
      {
        return shardSpecs.get(0);
      }
    };
  }

  @Override
  public boolean equals(Object obj)
  {
    return obj instanceof NoneShardSpec;
  }

  @Override
  public String toString()
  {
    return "NoneShardSpec";
  }
}
