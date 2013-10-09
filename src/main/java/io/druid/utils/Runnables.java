package io.druid.utils;

/**
 */
public class Runnables
{
  public static Runnable getNoopRunnable(){
    return new Runnable(){
      public void run(){}
    };
  }
}
