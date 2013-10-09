package io.druid.guice;

import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scope;
import com.google.inject.Scopes;

/**
 */
public class DruidScopes
{
  public static final Scope SINGLETON = new Scope()
  {
    @Override
    public <T> Provider<T> scope(Key<T> key, Provider<T> unscoped)
    {
      return Scopes.SINGLETON.scope(key, unscoped);
    }

    @Override
    public String toString()
    {
      return "DruidScopes.SINGLETON";
    }
  };
}
