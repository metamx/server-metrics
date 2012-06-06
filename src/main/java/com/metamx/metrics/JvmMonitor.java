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

import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.Map;

public class JvmMonitor extends AbstractMonitor
{
  private final KeyedDiff gcDiff = new KeyedDiff();

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    // I have no idea why, but jvm/mem is slightly more than the sum of jvm/pool. Let's just include
    // them both.

    // jvm/mem
    final Map<String, MemoryUsage> usages = ImmutableMap.of(
        "heap",    ManagementFactory.getMemoryMXBean().getHeapMemoryUsage(),
        "nonheap", ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage()
    );
    for (Map.Entry<String, MemoryUsage> entry : usages.entrySet()) {
      final String kind = entry.getKey();
      final MemoryUsage usage = entry.getValue();
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
          .setUser1(kind);
      emitter.emit(builder.build("jvm/mem/max",       usage.getMax()));
      emitter.emit(builder.build("jvm/mem/committed", usage.getCommitted()));
      emitter.emit(builder.build("jvm/mem/used",      usage.getUsed()));
      emitter.emit(builder.build("jvm/mem/init",      usage.getInit()));
    }

    // jvm/pool
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      final String kind = pool.getType() == MemoryType.HEAP ? "heap" : "nonheap";
      final MemoryUsage usage = pool.getUsage();
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
          .setUser1(kind)
          .setUser2(pool.getName());
      emitter.emit(builder.build("jvm/pool/max",       usage.getMax()));
      emitter.emit(builder.build("jvm/pool/committed", usage.getCommitted()));
      emitter.emit(builder.build("jvm/pool/used",      usage.getUsed()));
      emitter.emit(builder.build("jvm/pool/init",      usage.getInit()));
    }

    // jvm/gc
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      final Map<String, Long> diff = gcDiff.to(gc.getName(), ImmutableMap.of(
          "jvm/gc/time",  gc.getCollectionTime(),
          "jvm/gc/count", gc.getCollectionCount()
      ));
      if (diff != null) {
        final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
            .setUser1(gc.getName());
        for (Map.Entry<String, Long> entry : diff.entrySet()) {
          emitter.emit(builder.build(entry.getKey(), entry.getValue()));
        }
      }
    }

    return true;
  }
}
