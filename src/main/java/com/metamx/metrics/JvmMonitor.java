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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.Map;
import org.hyperic.sigar.ProcCpu;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

public class JvmMonitor extends AbstractMonitor
{
  private static final Logger log = new Logger(JvmMonitor.class);

  private final KeyedDiff keyDiff = new KeyedDiff();

  private final Sigar sigar = new Sigar();

  private Map<String, String[]> dimensions;

  public JvmMonitor()
  {
    this(ImmutableMap.<String, String[]>of());
  }

  public JvmMonitor(Map<String, String[]> dimensions)
  {
    Preconditions.checkNotNull(dimensions);
    this.dimensions = ImmutableMap.copyOf(dimensions);
  }

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
          .setDimension("memKind", kind);
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

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
          .setDimension("poolKind", kind)
          .setDimension("poolName", pool.getName());
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      emitter.emit(builder.build("jvm/pool/max",       usage.getMax()));
      emitter.emit(builder.build("jvm/pool/committed", usage.getCommitted()));
      emitter.emit(builder.build("jvm/pool/used",      usage.getUsed()));
      emitter.emit(builder.build("jvm/pool/init",      usage.getInit()));
    }

    // jvm/gc
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      final Map<String, Long> diff = this.keyDiff.to(
          gc.getName(), ImmutableMap.of(
              "jvm/gc/time", gc.getCollectionTime(),
              "jvm/gc/count", gc.getCollectionCount()
          )
      );
      if (diff != null) {
        final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
            .setDimension("gcName", gc.getName());
        MonitorUtils.addDimensionsToBuilder(builder, dimensions);

        for (Map.Entry<String, Long> entry : diff.entrySet()) {
          emitter.emit(builder.build(entry.getKey(), entry.getValue()));
        }
      }
    }

    // direct memory usage
    for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
          .setDimension("bufferpoolName", pool.getName());
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      emitter.emit(builder.build("jvm/bufferpool/capacity", pool.getTotalCapacity()));
      emitter.emit(builder.build("jvm/bufferpool/used", pool.getMemoryUsed()));
      emitter.emit(builder.build("jvm/bufferpool/count", pool.getCount()));
    }

    // process CPU
    try {
      ProcCpu procCpu = sigar.getProcCpu(sigar.getPid());
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);
      // delta for total, sys, user
      Map<String, Long> procDiff = this.keyDiff.to(
          "proc/cpu", ImmutableMap.of(
              "jvm/cpu/total", procCpu.getTotal(),
              "jvm/cpu/sys", procCpu.getSys(),
              "jvm/cpu/user", procCpu.getUser()
          )
      );
      if (procDiff != null) {
        for (Map.Entry<String, Long> entry : procDiff.entrySet()) {
          emitter.emit(builder.build(entry.getKey(), entry.getValue()));
        }
      }
      emitter.emit(builder.build("jvm/cpu/percent", procCpu.getPercent()));
    }
    catch (SigarException e) {
      log.error(e, "Failed to get ProcCpu");
    }
    return true;
  }

}
