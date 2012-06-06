/*
 * Copyright 2012 Metamarkets Group Inc.
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

import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.service.ServiceEmitter;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class MonitorScheduler
{
  private final MonitorSchedulerConfig config;
  private final ScheduledExecutorService exec;
  private final ServiceEmitter emitter;
  private final List<Monitor> monitors;

  private volatile boolean started = false;

  public MonitorScheduler(
      MonitorSchedulerConfig config,
      ScheduledExecutorService exec,
      ServiceEmitter emitter,
      List<Monitor> monitors
  )
  {
    this.config = config;
    this.exec = exec;
    this.emitter = emitter;
    this.monitors = monitors;
  }

  @LifecycleStart
  public synchronized void start()
  {
    if (started) {
      return;
    }
    started = true;

    for (final Monitor monitor : monitors) {
      monitor.start();
      ScheduledExecutors.scheduleAtFixedRate(
          exec,
          config.getEmitterPeriod(),
          new Callable<ScheduledExecutors.Signal>()
          {
            @Override
            public ScheduledExecutors.Signal call() throws Exception
            {
              return monitor.monitor(emitter) ? ScheduledExecutors.Signal.REPEAT : ScheduledExecutors.Signal.STOP;
            }
          }
      );
    }
  }

  @LifecycleStop
  public synchronized void stop()
  {
    if (!started) {
      return;
    }

    started = false;
    for (Monitor monitor : monitors) {
      monitor.stop();
    }
  }
}
