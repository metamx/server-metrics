/*
 * Copyright 2016 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.metamx.metrics;

import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class JvmMonitorTest
{

  @Test(timeout = 5000)
  public void testGcCounts()
  {
    GcTrackingEmitter emitter = new GcTrackingEmitter();

    final ServiceEmitter serviceEmitter = new ServiceEmitter("test", "localhost", emitter);
    serviceEmitter.start();
    final JvmMonitor jvmMonitor = new JvmMonitor();

    while (true) {
      // generate some garbage to see gc counters incremented
      @SuppressWarnings("unused")
      byte[] b = new byte[1024 * 1024 * 1024];
      emitter.reset();
      jvmMonitor.doMonitor(serviceEmitter);
      if (emitter.gcSeen())
        return;
    }
  }

  private static class GcTrackingEmitter implements Emitter
  {
    private Number oldGcCount;
    private Number oldGcCpu;
    private Number youngGcCount;
    private Number youngGcCpu;

    @Override
    public void start()
    {

    }

    void reset()
    {
      oldGcCount = null;
      oldGcCpu = null;
      youngGcCount = null;
      youngGcCpu = null;
    }

    @Override
    public void emit(Event e)
    {
      ServiceMetricEvent event = (ServiceMetricEvent) e;
      switch (event.getMetric()) {
        case "jvm/gc/old/count":
          oldGcCount = event.getValue();
          break;
        case "jvm/gc/old/cpu":
          oldGcCpu = event.getValue();
          break;
        case "jvm/gc/young/count":
          youngGcCount = event.getValue();
          break;
        case "jvm/gc/young/cpu":
          youngGcCpu = event.getValue();
          break;
      }
    }

    boolean gcSeen()
    {
      boolean gcSeen = false;
      if (oldGcCount != null && oldGcCount.longValue() > 0) {
        gcSeen = true;
        String message = "expected to see positive old gc cpu after some gc count";
        Assert.assertNotNull(message, oldGcCpu);
        Assert.assertTrue(message, Long.parseLong(oldGcCpu.toString()) > 0);
        System.out.println("old: " + oldGcCount + " " + oldGcCpu);
      }
      if (youngGcCount != null && youngGcCount.longValue() > 0) {
        gcSeen = true;
        String message = "expected to see positive young gc cpu after some gc count";
        Assert.assertNotNull(message, youngGcCpu);
        Assert.assertTrue(message, Long.parseLong(youngGcCpu.toString()) > 0);
        System.out.println("young: " + youngGcCount + " " + youngGcCpu);
      }
      return gcSeen;
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
}
