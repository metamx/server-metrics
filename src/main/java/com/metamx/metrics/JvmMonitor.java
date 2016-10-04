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
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.gridkit.lab.jvm.perfdata.JStatData;
import org.gridkit.lab.jvm.perfdata.JStatData.LongCounter;
import org.gridkit.lab.jvm.perfdata.JStatData.StringCounter;
import org.gridkit.lab.jvm.perfdata.JStatData.TickCounter;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.Map;

public class JvmMonitor extends AbstractMonitor
{
  /*
   * The following GC-related code is partially based on
   * https://github.com/aragozin/jvm-tools/blob/e0e37692648951440aa1a4ea5046261cb360df70/
   * sjk-core/src/main/java/org/gridkit/jvmtool/PerfCounterGcCpuUsageMonitor.java
   */

  private enum GcGeneration
  {
    YOUNG(0) {
      @Override
      String readableGcName(String name)
      {
        switch (name) {
          case "Copy": return "serial";
          case "PSScavenge": return "parallel";
          case "PCopy": return "cms";
          case "G1 incremental collections": return "g1";
          default: return name;
        }
      }
    },
    OLD(1) {
      @Override
      String readableGcName(String name)
      {
        switch (name) {
          case "MCS": return "serial";
          case "PSParallelCompact": return "parallel";
          case "CMS": return "cms";
          case "G1 stop-the-world full collections": return "g1";
          default: return name;
        }
      }
    };

    final int jStatOrder;

    GcGeneration(int jStatOrder)
    {
      this.jStatOrder = jStatOrder;
    }

    abstract String readableGcName(String name);
  }

  private static class GcCounters
  {
    final GcGeneration generation;
    final LongCounter invocations;
    final TickCounter cpu;
    final String readableGcName;
    long lastInvocations = 0;
    long lastCpuNanos = 0;

    GcCounters(Map<String, JStatData.Counter<?>> jStatCounters, GcGeneration generation)
    {
      this.generation = generation;
      int jStatOrder = generation.jStatOrder;
      // Names of counters could also be found at
      // http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/be698ac28848/
      // src/share/classes/sun/tools/jstat/resources/jstat_options
      invocations = (LongCounter) jStatCounters.get(String.format("sun.gc.collector.%d.invocations", jStatOrder));
      cpu = (TickCounter) jStatCounters.get(String.format("sun.gc.collector.%d.time", jStatOrder));
      String gcName = ((StringCounter) jStatCounters.get(String.format(
          "sun.gc.collector.%d.name",
          jStatOrder
      ))).getString();
      readableGcName = generation.readableGcName(gcName);
    }

    void emitCounters(ServiceEmitter emitter, ServiceMetricEvent.Builder metricBuilder)
    {
      metricBuilder.setDimension("gcGen", generation.name().toLowerCase());
      metricBuilder.setDimension("gcName", readableGcName);
      emitInvocations(emitter, metricBuilder);
      emitCpu(emitter, metricBuilder);
    }

    private void emitInvocations(ServiceEmitter emitter, ServiceMetricEvent.Builder metricBuilder)
    {
      long newInvocations = invocations.getLong();
      emitter.emit(metricBuilder.build("jvm/gc/count", newInvocations - lastInvocations));
      lastInvocations = newInvocations;
    }

    private void emitCpu(ServiceEmitter emitter, ServiceMetricEvent.Builder metricBuilder)
    {
      long newCpuNanos = cpu.getNanos();
      emitter.emit(metricBuilder.build("jvm/gc/cpu", newCpuNanos - lastCpuNanos));
      lastCpuNanos = newCpuNanos;
    }
  }

  private final GcCounters youngGcCounters;
  private final GcCounters oldGcCounters;

  {
    long currentProcessId = SigarUtil.getCurrentProcessId();
    // connect to itself
    JStatData jStatData = JStatData.connect(currentProcessId);
    Map<String, JStatData.Counter<?>> jStatCounters = jStatData.getAllCounters();
    youngGcCounters = new GcCounters(jStatCounters, GcGeneration.YOUNG);
    oldGcCounters = new GcCounters(jStatCounters, GcGeneration.OLD);
  }


  private final Map<String, String[]> dimensions;

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
    emitGcMetrics(youngGcCounters, emitter);
    emitGcMetrics(oldGcCounters, emitter);

    // direct memory usage
    for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
          .setDimension("bufferpoolName", pool.getName());
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      emitter.emit(builder.build("jvm/bufferpool/capacity", pool.getTotalCapacity()));
      emitter.emit(builder.build("jvm/bufferpool/used", pool.getMemoryUsed()));
      emitter.emit(builder.build("jvm/bufferpool/count", pool.getCount()));
    }

    return true;
  }

  private void emitGcMetrics(GcCounters gcCounters, ServiceEmitter emitter)
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    MonitorUtils.addDimensionsToBuilder(builder, dimensions);
    gcCounters.emitCounters(emitter, builder);
  }
}
