/*
 * Copyright 2017 Metamarkets Group Inc.
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

import com.google.common.base.Throwables;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.joda.time.DateTime;

public class ProcDiskStatsDeltaMonitor extends FeedDefiningMonitor
{
  private static final Logger log = new Logger(ProcDiskStatsDeltaMonitor.class);
  private final AtomicReference<ProcDiskStatsHolder> procDiskStatsHolder = new AtomicReference<>(null);
  private final Map<String, String[]> dimensions;

  public ProcDiskStatsDeltaMonitor(String feed, Map<String, String[]> dimensions)
  {
    super(feed);
    this.dimensions = dimensions;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final ProcDiskStatsHolder priorHolder = procDiskStatsHolder.get();
    final ProcDiskStats diskStats;
    final DateTime dateTime;
    final long time;
    try {
      final List<String> lines = Files.readAllLines(Paths.get("/proc/diskstats"));
      time = System.nanoTime();
      dateTime = new DateTime(); // As close to read-time as possible
      diskStats = ProcDiskStats.parse(lines);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }


    if (!procDiskStatsHolder.compareAndSet(priorHolder, new ProcDiskStatsHolder(diskStats, time))) {
      log.warn("Lost race for reporting metrics, skipping reporting");
      return false;
    }

    // Totals to begin with, regardless of if its the first report
    diskStats.getDiskMetrics().forEach((metric) -> {
      final ServiceMetricEvent.Builder builder = builder().setDimension("device", metric.getDevice());
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);
      emitter.emit(builder.build(dateTime, "sys/disk/read/complete/total", metric.getrComplete()));
      emitter.emit(builder.build(dateTime, "sys/disk/read/merge/total", metric.getrMerge()));
      emitter.emit(builder.build(dateTime, "sys/disk/read/sector/total", metric.getrSector()));
      emitter.emit(builder.build(dateTime, "sys/disk/read/timeMs/total", metric.getrTimeMs()));

      emitter.emit(builder.build(dateTime, "sys/disk/write/complete/total", metric.getwComplete()));
      emitter.emit(builder.build(dateTime, "sys/disk/write/merge/total", metric.getwMerge()));
      emitter.emit(builder.build(dateTime, "sys/disk/write/sector/total", metric.getwSector()));
      emitter.emit(builder.build(dateTime, "sys/disk/write/timeMs/total", metric.getwTimeMs()));

      emitter.emit(builder.build(dateTime, "sys/disk/timeMs/total", metric.getActiveTimeMs()));
      emitter.emit(builder.build(dateTime, "sys/disk/weightedTimeMs/total", metric.getWeightedActiveTimeMs()));

      // This one is absent from delta
      emitter.emit(builder.build(dateTime, "sys/disk/queue", metric.getActiveCount()));
    });
    if (priorHolder == null) {
      log.debug("Skipping delta for first report");
      return true;
    }
    final Map<String, ProcDiskStats.DiskMetric> priorDiskMetrics = priorHolder
        .diskStats
        .getDiskMetrics()
        .stream()
        .collect(Collectors.toMap(ProcDiskStats.DiskMetric::getDevice, Function.identity()));

    // Deltas only for things which are present in both, and we're not on the first report
    diskStats.getDiskMetrics().forEach((metric) -> {
      final ProcDiskStats.DiskMetric priorDiskMetric = priorDiskMetrics.get(metric.getDevice());
      if (priorDiskMetric == null) {
        log.warn("Skipping delta for new device [%s]", metric.getDevice());
        return;
      }
      final ProcDiskStats.DiskMetric delta = metric.deltaSince(priorDiskMetric);
      final ServiceMetricEvent.Builder builder = builder().setDimension("device", delta.getDevice());
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);
      emitter.emit(builder.build(dateTime, "sys/disk/read/complete/delta", delta.getrComplete()));
      emitter.emit(builder.build(dateTime, "sys/disk/read/merge/delta", delta.getrMerge()));
      emitter.emit(builder.build(dateTime, "sys/disk/read/sector/delta", delta.getrSector()));
      emitter.emit(builder.build(dateTime, "sys/disk/read/timeMs/total", delta.getrTimeMs()));

      emitter.emit(builder.build(dateTime, "sys/disk/write/complete/delta", delta.getwComplete()));
      emitter.emit(builder.build(dateTime, "sys/disk/write/merge/delta", delta.getwMerge()));
      emitter.emit(builder.build(dateTime, "sys/disk/write/sector/delta", delta.getwSector()));
      emitter.emit(builder.build(dateTime, "sys/disk/write/timeMs/delta", delta.getwTimeMs()));

      emitter.emit(builder.build(dateTime, "sys/disk/timeMs/delta", delta.getActiveTimeMs()));
      emitter.emit(builder.build(dateTime, "sys/disk/weightedTimeMs/delta", delta.getWeightedActiveTimeMs()));

      emitter.emit(builder.build(dateTime, "sys/disk/metricTimeNs/delta", time - priorHolder.time));
    });
    return true;
  }

  static class ProcDiskStatsHolder
  {
    private final ProcDiskStats diskStats;
    private final long time;

    private ProcDiskStatsHolder(ProcDiskStats diskStats, long time)
    {
      this.diskStats = diskStats;
      this.time = time;
    }
  }
}
