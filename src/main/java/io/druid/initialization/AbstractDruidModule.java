package io.druid.initialization;

/**
 *
 */
public abstract class AbstractDruidModule implements DruidModule
{
  // These are to make sure modules shoved into a hash set are unique on class rather than instance.
  @Override
  public int hashCode(){
    return getClass().getCanonicalName().hashCode();
  }
  @Override
  public boolean equals(Object other){
    if(null == other){
      return false;
    }
    return other.getClass().equals(getClass());
  }
}
