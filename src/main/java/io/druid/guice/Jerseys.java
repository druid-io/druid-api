package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import io.druid.guice.annotations.JSR311Resource;

/**
 */
public class Jerseys
{
  public static void addResource(Binder binder, Class<?> resourceClazz)
  {
    Multibinder.newSetBinder(binder, new TypeLiteral<Class<?>>(){}, JSR311Resource.class)
               .addBinding()
               .toInstance(resourceClazz);
  }
}
