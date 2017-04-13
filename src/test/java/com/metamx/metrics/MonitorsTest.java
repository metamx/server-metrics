package com.metamx.metrics;

import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.ServiceEmitter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class MonitorsTest
{
  private static class StubServiceEmitter extends ServiceEmitter
  {
    private List<Event> events = new ArrayList<>();

    public StubServiceEmitter(String service, String host)
    {
      super(service, host, null);
    }

    @Override
    public void emit(Event event)
    {
      events.add(event);
    }

    public List<Event> getEvents()
    {
      return events;
    }

    @Override
    public void start()
    {
    }

    @Override
    public void flush() throws IOException
    {
    }

    @Override
    public void close() throws IOException
    {
    }
  }

  @Test
  public void testSetFeed()
  {
    String feed = "testFeed";
    StubServiceEmitter emitter = new StubServiceEmitter("dev/monitor-test", "localhost:0000");
    Monitor m = Monitors.createCompoundJvmMonitor(ImmutableMap.<String, String[]>of(), feed);
    m.start();
    m.monitor(emitter);
    m.stop();
    checkEvents(emitter.getEvents(), feed);
  }

  @Test
  public void testDefaultFeed()
  {
    StubServiceEmitter emitter = new StubServiceEmitter("dev/monitor-test", "localhost:0000");
    Monitor m = Monitors.createCompoundJvmMonitor(ImmutableMap.<String, String[]>of());
    m.start();
    m.monitor(emitter);
    m.stop();
    checkEvents(emitter.getEvents(), "metrics");
  }

  private void checkEvents(List<Event> events, String expectedFeed)
  {
    Assert.assertFalse("no events emitted", events.isEmpty());
    for (Event e : events) {
      if (!expectedFeed.equals(e.getFeed())) {
        String message = String.format("\"feed\" in event: %s", e.toMap().toString());
        Assert.assertEquals(message, expectedFeed, e.getFeed());
      }
    }
  }
}
