package io.druid.initialization;

import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import io.druid.query.Query;
import io.druid.query.QueryRunnerFactory;

/**
 */
public class Binders
{
  public static MapBinder<Class<? extends Query>, QueryRunnerFactory> queryFactoryBinder(Binder binder)
  {
    return MapBinder.newMapBinder(
        binder, new TypeLiteral<Class<? extends Query>>(){}, new TypeLiteral<QueryRunnerFactory>(){}
    );
  }
}
