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
import org.gridkit.lab.jvm.perfdata.JStatData.TickCounter;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.Map;

public class JvmMonitor extends AbstractMonitor
{
  /**
   * The following code is partially based on
   * https://github.com/aragozin/jvm-tools/blob/e0e37692648951440aa1a4ea5046261cb360df70/
   * sjk-core/src/main/java/org/gridkit/jvmtool/PerfCounterGcCpuUsageMonitor.java
   */
  private final TickCounter youngGcCpu;
  private final LongCounter youngGcInvocations;
  private final TickCounter oldGcCpu;
  private final LongCounter oldGcInvocations;

  {
    long currentProcessId = SigarUtil.getSigar().getPid();
    // connect to itself
    JStatData jStatData = JStatData.connect(currentProcessId);
    Map<String, JStatData.Counter<?>> jStatCounters = jStatData.getAllCounters();
    // Names of counters could also be found at
    // http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/be698ac28848/
    // src/share/classes/sun/tools/jstat/resources/jstat_options
    youngGcCpu = (TickCounter) jStatCounters.get("sun.gc.collector.0.time");
    youngGcInvocations = (LongCounter) jStatCounters.get("sun.gc.collector.0.invocations");
    oldGcCpu = (TickCounter) jStatCounters.get("sun.gc.collector.1.time");
    oldGcInvocations = (LongCounter) jStatCounters.get("sun.gc.collector.1.invocations");
  }



  private Map<String, Long> lastGcCounters = null;
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

    emitGcMetrics(emitter);

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

  private void emitGcMetrics(ServiceEmitter emitter)
  {
    ImmutableMap<String, Long> newGcCounters = ImmutableMap.of(
      "jvm/gc/young/cpu", youngGcCpu.getNanos(),
      "jvm/gc/young/count", youngGcInvocations.getLong(),
      "jvm/gc/old/cpu", oldGcCpu.getNanos(),
      "jvm/gc/old/count", oldGcInvocations.getLong()
    );

    final Map<String, Long> diff;
    if (lastGcCounters != null) {
      diff = KeyedDiff.subtract(newGcCounters, lastGcCounters);
    } else {
      diff = newGcCounters;
    }
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    MonitorUtils.addDimensionsToBuilder(builder, dimensions);

    for (Map.Entry<String, Long> entry : diff.entrySet()) {
      emitter.emit(builder.build(entry.getKey(), entry.getValue()));
    }
    lastGcCounters = newGcCounters;
  }
}
