package io.druid.initialization;

import com.fasterxml.jackson.databind.Module;

import java.util.List;

/**
 */
public interface DruidModule extends com.google.inject.Module
{
  public List<Module> getJacksonModules();
}
