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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.common.RE;
import java.util.Collection;
import java.util.List;

public class ProcDiskStats
{
  private final Collection<DiskMetric> diskMetrics;

  static ProcDiskStats parse(List<String> lines)
  {
    return new ProcDiskStats(Lists.transform(lines, ProcDiskStats::parseDiskMetric));
  }

  private static DiskMetric parseDiskMetric(String line)
  {
    final String[] entities = line.trim().split("  *", 15);
    if (entities.length != 14) {
      throw new RE("Invalid disk metric [%s]", line);
    }
    // This offset is so that the docs match with the parameters
    final int offset = 2;
    return new DiskMetric(
        entities[2],
        Long.parseLong(entities[offset + 1]),
        Long.parseLong(entities[offset + 2]),
        Long.parseLong(entities[offset + 3]),
        Long.parseLong(entities[offset + 4]),
        Long.parseLong(entities[offset + 5]),
        Long.parseLong(entities[offset + 6]),
        Long.parseLong(entities[offset + 7]),
        Long.parseLong(entities[offset + 8]),
        Long.parseLong(entities[offset + 9]),
        Long.parseLong(entities[offset + 10]),
        Long.parseLong(entities[offset + 11])
    );
  }

  private ProcDiskStats(Collection<DiskMetric> diskMetrics)
  {
    this.diskMetrics = ImmutableList.copyOf(diskMetrics);
  }

  public Collection<DiskMetric> getDiskMetrics()
  {
    return diskMetrics;
  }

  // See https://www.kernel.org/doc/Documentation/iostats.txt
  public static class DiskMetric
  {
    private final String device;
    private final long rComplete;
    private final long rMerge;
    private final long rSector;
    private final long rTimeMs;
    private final long wComplete;
    private final long wMerge;
    private final long wSector;
    private final long wTimeMs;
    private final long activeCount;
    private final long activeTimeMs;
    private final long weightedActiveTimeMs;

    @VisibleForTesting
    DiskMetric(
        String device,
        long rComplete, long rMerge, long rSector, long rTimeMs,
        long wComplete, long wMerge, long wSector, long wTimeMs,
        long activeCount, long activeTimeMs, long weightedActiveTimeMs
    )
    {
      this.device = device;
      this.rComplete = rComplete;
      this.rMerge = rMerge;
      this.rSector = rSector;
      this.rTimeMs = rTimeMs;
      this.wComplete = wComplete;
      this.wMerge = wMerge;
      this.wSector = wSector;
      this.wTimeMs = wTimeMs;
      this.activeCount = activeCount;
      this.activeTimeMs = activeTimeMs;
      this.weightedActiveTimeMs = weightedActiveTimeMs;
    }

    public DiskMetric deltaSince(DiskMetric other)
    {
      Preconditions.checkArgument(
          device.equals(other.device),
          "Devices must match. [%s] != [%s]",
          device,
          other.device
      );
      return new DiskMetric(
          device,
          rComplete - other.rComplete,
          rMerge - other.rMerge,
          rSector - other.rSector,
          rTimeMs - other.rTimeMs,
          wComplete - other.wComplete,
          wMerge - other.wMerge,
          wSector - other.wSector,
          wTimeMs - other.wTimeMs,
          0,
          activeTimeMs - other.activeTimeMs,
          weightedActiveTimeMs - other.weightedActiveTimeMs
      );
    }

    public String getDevice()
    {
      return device;
    }

    public long getrComplete()
    {
      return rComplete;
    }

    public long getrMerge()
    {
      return rMerge;
    }

    public long getrSector()
    {
      return rSector;
    }

    public long getrTimeMs()
    {
      return rTimeMs;
    }

    public long getwComplete()
    {
      return wComplete;
    }

    public long getwMerge()
    {
      return wMerge;
    }

    public long getwSector()
    {
      return wSector;
    }

    public long getwTimeMs()
    {
      return wTimeMs;
    }

    public long getActiveCount()
    {
      return activeCount;
    }

    public long getActiveTimeMs()
    {
      return activeTimeMs;
    }

    public long getWeightedActiveTimeMs()
    {
      return weightedActiveTimeMs;
    }
  }
}
