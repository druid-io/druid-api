package io.druid.cli;

import io.airlift.command.Cli;

/**
 */
public interface CliCommandCreator
{
  public void addCommands(Cli.CliBuilder builder);
}
