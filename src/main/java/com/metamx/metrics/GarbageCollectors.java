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

import com.metamx.common.logger.Logger;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

final class GarbageCollectors
{
  private static final Logger log = new Logger(GarbageCollectors.class);
  private static final String young;
  private static final String old;

  static {
    List<String> gcNames = new ArrayList<>();
    for (GarbageCollectorMXBean gcMXBean : ManagementFactory.getPlatformMXBeans(GarbageCollectorMXBean.class)) {
      gcNames.add(gcMXBean.getName());
    }
    if (gcNames.size() != 2) {
      log.error("Expected exactly 2 GCs, seen: " + gcNames);
      young = "unknown";
      old = "unknown";
    } else {
      {
        String youngGcName = gcNames.get(0);
        if (youngGcName.contains("Copy")) {
          young = "copy";
        } else if (youngGcName.contains("Scavenge")) {
          young = "scavenge";
        } else if (youngGcName.contains("ParNew")) {
          young = "parNew";
        } else if (youngGcName.contains("G1")) {
          young = "g1";
        } else {
          log.error("Unknown young GC name: " + youngGcName);
          young = "unknown";
        }
      }
      {
        String oldGcName = gcNames.get(1);
        if (oldGcName.contains("MarkSweepCompact")) {
          old = "serial";
        } else if (oldGcName.contains("PS MarkSweep")) {
          old = "parallel";
        } else if (oldGcName.contains("ConcurrentMarkSweep")) {
          old = "cms";
        } else if (oldGcName.contains("G1")) {
          old = "g1";
        } else {
          log.error("Unknown old GC name: " + oldGcName);
          old = "unknown";
        }
      }
    }
  }

  static String youngName()
  {
    return young;
  }

  static String oldName()
  {
    return old;
  }

  private GarbageCollectors() {}
}
