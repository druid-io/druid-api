package io.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.data.input.InputRow;

import java.util.List;

/**
 * A Marker interface that exists to combine ShardSpec objects together for Jackson
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
                  @JsonSubTypes.Type(name = "none", value = NoneShardSpec.class),
              })
public interface ShardSpec
{
  public <T> PartitionChunk<T> createChunk(T obj);

  public boolean isInChunk(long timestamp, InputRow inputRow);

  public int getPartitionNum();

  public ShardSpecLookup getLookup(List<ShardSpec> shardSpecs);
}
