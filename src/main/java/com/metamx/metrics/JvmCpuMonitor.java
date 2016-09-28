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
import java.util.Map;
import org.hyperic.sigar.ProcCpu;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

public class JvmCpuMonitor extends AbstractMonitor
{
  private static final Logger log = new Logger(JvmCpuMonitor.class);

  private final Sigar sigar = SigarUtil.getSigar();
  private final long currentProcessId = sigar.getPid();

  private final KeyedDiff diff = new KeyedDiff();

  private Map<String, String[]> dimensions;

  public JvmCpuMonitor()
  {
    this(ImmutableMap.<String, String[]>of());
  }

  public JvmCpuMonitor(Map<String, String[]> dimensions)
  {
    Preconditions.checkNotNull(dimensions);
    this.dimensions = ImmutableMap.copyOf(dimensions);
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    // process CPU
    try {
      ProcCpu procCpu = sigar.getProcCpu(currentProcessId);
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);
      // delta for total, sys, user
      Map<String, Long> procDiff = diff.to(
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
