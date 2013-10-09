package io.druid.guice;

import com.google.common.collect.Iterables;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

/**
 */
public class PolyBindTest
{
  private Properties props;
  private Injector injector;

  public void setUp(Module... modules) throws Exception
  {
    props = new Properties();
    injector = Guice.createInjector(
        Iterables.concat(
            Arrays.asList(
                new Module()
                {
                  @Override
                  public void configure(Binder binder)
                  {
                    binder.bind(Properties.class).toInstance(props);
                    PolyBind.createChoice(binder, "billy", Key.get(Gogo.class), Key.get(GoA.class));
                  }
                }
            ),
            Arrays.asList(modules)
        )
    );
  }

  @Test
  public void testSanity() throws Exception
  {
    setUp(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            final MapBinder<String,Gogo> gogoBinder = PolyBind.optionBinder(binder, Key.get(Gogo.class));
            gogoBinder.addBinding("a").to(GoA.class);
            gogoBinder.addBinding("b").to(GoB.class);

            PolyBind.createChoice(
                binder, "billy", Key.get(Gogo.class, Names.named("reverse")), Key.get(GoB.class)
            );
            final MapBinder<String,Gogo> annotatedGogoBinder = PolyBind.optionBinder(
                binder, Key.get(Gogo.class, Names.named("reverse"))
            );
            annotatedGogoBinder.addBinding("a").to(GoB.class);
            annotatedGogoBinder.addBinding("b").to(GoA.class);
          }
        }
    );


    Assert.assertEquals("A", injector.getInstance(Gogo.class).go());
    Assert.assertEquals("B", injector.getInstance(Key.get(Gogo.class, Names.named("reverse"))).go());
    props.setProperty("billy", "b");
    Assert.assertEquals("B", injector.getInstance(Gogo.class).go());
    Assert.assertEquals("A", injector.getInstance(Key.get(Gogo.class, Names.named("reverse"))).go());
    props.setProperty("billy", "a");
    Assert.assertEquals("A", injector.getInstance(Gogo.class).go());
    Assert.assertEquals("B", injector.getInstance(Key.get(Gogo.class, Names.named("reverse"))).go());
    props.setProperty("billy", "b");
    Assert.assertEquals("B", injector.getInstance(Gogo.class).go());
    Assert.assertEquals("A", injector.getInstance(Key.get(Gogo.class, Names.named("reverse"))).go());
    props.setProperty("billy", "c");
    Assert.assertEquals("A", injector.getInstance(Gogo.class).go());
    Assert.assertEquals("B", injector.getInstance(Key.get(Gogo.class, Names.named("reverse"))).go());
  }

  public static interface Gogo
  {
    public String go();
  }

  public static class GoA implements Gogo
  {
    @Override
    public String go()
    {
      return "A";
    }
  }

  public static class GoB implements Gogo
  {
    @Override
    public String go()
    {
      return "B";
    }
  }
}
