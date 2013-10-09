package io.druid.timeline.partition;

import io.druid.data.input.InputRow;

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
  public boolean isInChunk(InputRow inputRow)
  {
    return true;
  }

  @Override
  public int getPartitionNum()
  {
    return 0;
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
