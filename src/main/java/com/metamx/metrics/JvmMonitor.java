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
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.gridkit.lab.jvm.perfdata.JStatData;
import org.gridkit.lab.jvm.perfdata.JStatData.LongCounter;
import org.gridkit.lab.jvm.perfdata.JStatData.StringCounter;
import org.gridkit.lab.jvm.perfdata.JStatData.TickCounter;

public class JvmMonitor extends AbstractMonitor
{
  private final Map<String, String[]> dimensions;

  private final GcCounters gcCounters = new GcCounters();

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
    // jvm/mem
    emitJvmMemMetrics(emitter);

    // direct memory usage
    emitDirectMemMetrics(emitter);

    // jvm/gc
    emitGcMetrics(emitter);

    return true;
  }

  @Deprecated
  private void emitJvmMemMetrics(ServiceEmitter emitter)
  {
    // I have no idea why, but jvm/mem is slightly more than the sum of jvm/pool. Let's just include
    // them both.
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
  }

  private void emitDirectMemMetrics(ServiceEmitter emitter)
  {

    for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
          .setDimension("bufferpoolName", pool.getName());
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      emitter.emit(builder.build("jvm/bufferpool/capacity", pool.getTotalCapacity()));
      emitter.emit(builder.build("jvm/bufferpool/used", pool.getMemoryUsed()));
      emitter.emit(builder.build("jvm/bufferpool/count", pool.getCount()));
    }
  }

  private void emitGcMetrics(ServiceEmitter emitter)
  {
    gcCounters.emit(emitter, dimensions);
  }

  /*
   * The following GC-related code is partially based on
   * https://github.com/aragozin/jvm-tools/blob/e0e37692648951440aa1a4ea5046261cb360df70/
   * sjk-core/src/main/java/org/gridkit/jvmtool/PerfCounterGcCpuUsageMonitor.java
   */
  private class GcCounters
  {
    private List<GcGeneration> generations = new ArrayList<>();

    GcCounters()
    {
      long currentProcessId = SigarUtil.getCurrentProcessId();
      // connect to itself
      JStatData jStatData = JStatData.connect(currentProcessId);
      Map<String, JStatData.Counter<?>> jStatCounters = jStatData.getAllCounters();

      generations.add(new GcGeneration(jStatCounters, 0, "young"));
      generations.add(new GcGeneration(jStatCounters, 1, "old"));
      // Removed in Java 8 but still actual for previous Java versions
      if (jStatCounters.containsKey("sun.gc.generation.2.name")) {
        generations.add(new GcGeneration(jStatCounters, 2, "perm"));
      }
    }

    void emit(ServiceEmitter emitter, Map<String, String[]> dimensions)
    {
      for (GcGeneration generation : generations) {
        final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
        MonitorUtils.addDimensionsToBuilder(builder, dimensions);
        generation.emit(emitter, builder);
      }
    }
  }

  private class GcGeneration
  {
    private String name;
    private GcGenerationCollector collector = null;
    private List<GcGenerationSpace> spaces = new ArrayList<>();

    GcGeneration(Map<String, JStatData.Counter<?>> jStatCounters, long genIndex, String name)
    {
      this.name = name.toLowerCase();

      long spacesCount = ((JStatData.LongCounter) jStatCounters.get(String.format("sun.gc.generation.%d.spaces", genIndex))).getLong();
      for (long spaceIndex = 0; spaceIndex < spacesCount; spaceIndex++) {
        spaces.add(new GcGenerationSpace(jStatCounters, genIndex, spaceIndex));
      }

      if (jStatCounters.containsKey(String.format("sun.gc.collector.%d.name", genIndex))) {
        collector = new GcGenerationCollector(jStatCounters, genIndex);
      }
    }

    void emit(ServiceEmitter emitter, ServiceMetricEvent.Builder builder)
    {
      builder.setDimension("gcGen", name);

      if (collector != null) {
        collector.emit(emitter, builder);
      }

      for (GcGenerationSpace space: spaces) {
        space.emit(emitter, builder);
      }
    }
  }

  private class GcGenerationCollector
  {
    private String name;
    private final LongCounter invocationsCounter;
    private final TickCounter cpuCounter;
    private long lastInvocations = 0;
    private long lastCpuNanos = 0;

    GcGenerationCollector(Map<String, JStatData.Counter<?>> jStatCounters, long genIndex)
    {
      String collectorKeyPrefix = String.format("sun.gc.collector.%d", genIndex);

      String nameKey = String.format("%s.name", collectorKeyPrefix);
      StringCounter nameCounter = (StringCounter) jStatCounters.get(nameKey);
      name = getReadableName(nameCounter.getString());

      invocationsCounter = (LongCounter) jStatCounters.get(String.format("%s.invocations", collectorKeyPrefix));
      cpuCounter = (TickCounter) jStatCounters.get(String.format("%s.time", collectorKeyPrefix));
    }

    void emit(ServiceEmitter emitter, ServiceMetricEvent.Builder builder)
    {
      builder.setDimension("gcName", name);

      long newInvocations = invocationsCounter.getLong();
      emitter.emit(builder.build("jvm/gc/count", newInvocations - lastInvocations));
      lastInvocations = newInvocations;

      long newCpuNanos = cpuCounter.getNanos();
      emitter.emit(builder.build("jvm/gc/cpu", newCpuNanos - lastCpuNanos));
      lastCpuNanos = newCpuNanos;
    }

    private String getReadableName(String name)
    {
      switch (name) {
        // Young gen
        case "Copy": return "serial";
        case "PSScavenge": return "parallel";
        case "PCopy": return "cms";
        case "G1 incremental collections": return "g1";

        // Old gen
        case "MCS": return "serial";
        case "PSParallelCompact": return "parallel";
        case "CMS": return "cms";
        case "G1 stop-the-world full collections": return "g1";

        default: return name;
      }
    }
  }

  private class GcGenerationSpace
  {
    private String name;

    private LongCounter maxCounter;
    private LongCounter capacityCounter;
    private LongCounter usedCounter;
    private LongCounter initCounter;

    GcGenerationSpace(Map<String, JStatData.Counter<?>> jStatCounters, long genIndex, long spaceIndex)
    {
      String spaceKeyPrefix = String.format("sun.gc.generation.%d.space.%d", genIndex, spaceIndex);

      String nameKey = String.format("%s.name", spaceKeyPrefix);
      StringCounter nameCounter = (StringCounter) jStatCounters.get(nameKey);
      name = nameCounter.toString().toLowerCase();

      maxCounter      = (LongCounter) jStatCounters.get(String.format("%s.maxCapacity",  spaceKeyPrefix));
      capacityCounter = (LongCounter) jStatCounters.get(String.format("%s.capacity",     spaceKeyPrefix));
      usedCounter     = (LongCounter) jStatCounters.get(String.format("%s.used",         spaceKeyPrefix));
      initCounter     = (LongCounter) jStatCounters.get(String.format("%s.initCapacity", spaceKeyPrefix));
    }

    void emit(ServiceEmitter emitter, ServiceMetricEvent.Builder builder)
    {
      builder.setDimension("gcGenSpaceName", name);

      emitter.emit(builder.build("jvm/gc/mem/max",      maxCounter.getLong()));
      emitter.emit(builder.build("jvm/gc/mem/capacity", capacityCounter.getLong()));
      emitter.emit(builder.build("jvm/gc/mem/used",     usedCounter.getLong()));
      emitter.emit(builder.build("jvm/gc/mem/init",     initCounter.getLong()));
    }
  }
}
