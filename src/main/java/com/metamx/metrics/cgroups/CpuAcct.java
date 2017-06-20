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

package com.metamx.metrics.cgroups;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.common.StringUtils;
import com.metamx.metrics.CgroupUtil;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.LongStream;

public class CpuAcct
{
  private static final String CGROUP = "cpuacct";
  private static final String CGROUP_ACCT_FILE = "cpuacct.usage_all";

  // Private because it requires a specific format and cant' take a generic list of strings
  private static CpuAcctMetric parse(final List<String> input)
  {
    // File has a header. We skip it
    // See src/test/resources/cpuacct.usage_all for an example
    final int ncpus = input.size() - 1;
    final long[] usr_time = new long[ncpus];
    final long[] sys_time = new long[ncpus];
    for (int i = 1; i < input.size(); ++i) {
      final String[] splits = input.get(i).split(CgroupUtil.SPACE_MATCH, 3);
      if (splits.length != 3) {
        throw new RuntimeException(StringUtils.safeFormat("Error parsing [%s]", input.get(i)));
      }
      final int cpu_num = Integer.parseInt(splits[0]);
      usr_time[cpu_num] = Long.parseLong(splits[1]);
      sys_time[cpu_num] = Long.parseLong(splits[2]);
    }
    return new CpuAcctMetric(usr_time, sys_time);
  }

  private final CgroupDiscoverer cgroupDiscoverer;
  private final PidDiscoverer pidDiscoverer;

  public CpuAcct(CgroupDiscoverer cgroupDiscoverer, PidDiscoverer pidDiscoverer)
  {
    this.cgroupDiscoverer = cgroupDiscoverer;
    this.pidDiscoverer = pidDiscoverer;
  }

  public CpuAcctMetric snapshot()
  {
    final File cpuacct = new File(cgroupDiscoverer.discover(CGROUP, pidDiscoverer.getPid()).toFile(), CGROUP_ACCT_FILE);
    try {
      return parse(Files.readAllLines(cpuacct.toPath(), Charsets.UTF_8));
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static class CpuAcctMetric
  {
    private final long[] usrTimes;
    private final long[] sysTimes;

    CpuAcctMetric(long[] usrTimes, long[] sysTimes)
    {
      this.usrTimes = usrTimes;
      this.sysTimes = sysTimes;
      Preconditions.checkArgument(usrTimes.length == sysTimes.length, "Lengths must match");
    }

    public final int cpuCount()
    {
      return usrTimes.length;
    }

    public final long[] sysTimes()
    {
      return sysTimes;
    }

    public final long[] usrTimes()
    {
      return usrTimes;
    }

    public final long usrTime(int cpu_num)
    {
      return usrTimes[cpu_num];
    }

    public final long sysTime(int cpu_num)
    {
      return sysTimes[cpu_num];
    }

    public final long usrTime()
    {
      return LongStream.of(usrTimes).sum();
    }

    public final long sysTime()
    {
      return LongStream.of(sysTimes).sum();
    }

    public final long time()
    {
      return usrTime() + sysTime();
    }

    public final CpuAcctMetric cumulativeSince(CpuAcctMetric other)
    {
      final int cpu_count = cpuCount();
      Preconditions.checkArgument(cpu_count == other.cpuCount(), "Cpu count missmatch");
      final long[] sys_times = new long[cpu_count];
      final long[] usr_times = new long[cpu_count];
      for (int i = 0; i < cpu_count; ++i) {
        sys_times[i] = this.sysTimes[i] - other.sysTimes[i];
        usr_times[i] = this.usrTimes[i] - other.usrTimes[i];
      }
      return new CpuAcctMetric(usr_times, sys_times);
    }
  }
}
