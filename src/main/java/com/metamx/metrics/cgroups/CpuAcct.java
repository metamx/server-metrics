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
import com.metamx.common.RE;
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
  private static CpuAcctMetric parse(final List<String> lines)
  {
    // File has a header. We skip it
    // See src/test/resources/cpuacct.usage_all for an example
    final int ncpus = lines.size() - 1;
    final long[] usrTime = new long[ncpus];
    final long[] sysTime = new long[ncpus];
    for (int i = 1; i < lines.size(); i++) {
      final String[] splits = lines.get(i).split(CgroupUtil.SPACE_MATCH, 3);
      if (splits.length != 3) {
        throw new RE("Error parsing [%s]", lines.get(i));
      }
      final int cpuNum = Integer.parseInt(splits[0]);
      usrTime[cpuNum] = Long.parseLong(splits[1]);
      sysTime[cpuNum] = Long.parseLong(splits[2]);
    }
    return new CpuAcctMetric(usrTime, sysTime);
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
      throw new RuntimeException(e);
    }
  }

  public static class CpuAcctMetric
  {
    private final long[] usrTimes;
    private final long[] sysTimes;

    CpuAcctMetric(long[] usrTimes, long[] sysTimes)
    {
      Preconditions.checkArgument(usrTimes.length == sysTimes.length, "Lengths must match");
      this.usrTimes = usrTimes;
      this.sysTimes = sysTimes;
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

    public final long usrTime(int cpuNum)
    {
      return usrTimes[cpuNum];
    }

    public final long sysTime(int cpu_Num)
    {
      return sysTimes[cpu_Num];
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
      final int cpuCount = cpuCount();
      Preconditions.checkArgument(cpuCount == other.cpuCount(), "Cpu count missmatch");
      final long[] sysTimes = new long[cpuCount];
      final long[] usrTimes = new long[cpuCount];
      for (int i = 0; i < cpuCount; i++) {
        sysTimes[i] = this.sysTimes[i] - other.sysTimes[i];
        usrTimes[i] = this.usrTimes[i] - other.usrTimes[i];
      }
      return new CpuAcctMetric(usrTimes, sysTimes);
    }
  }
}
