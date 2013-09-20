package io.druid.guice;

import com.google.inject.Key;

/**
 */
public class KeyHolder<T>
{
  private final Key<? extends T> key;

  public KeyHolder(
      Key<? extends T> key
  )
  {
    this.key = key;
  }

  public Key<? extends T> getKey()
  {
    return key;
  }
}
