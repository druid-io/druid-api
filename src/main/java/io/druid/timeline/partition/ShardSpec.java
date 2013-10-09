package io.druid.timeline.partition;

import io.druid.data.input.InputRow;

/**
 * A Marker interface that exists to combine ShardSpec objects together for Jackson
 */
public interface ShardSpec
{
  public <T> PartitionChunk<T> createChunk(T obj);
  public boolean isInChunk(InputRow inputRow);
  public int getPartitionNum();
}
