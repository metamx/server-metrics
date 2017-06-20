package com.metamx.metrics.cgroups;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import com.metamx.metrics.CgroupUtil;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.LongStream;

public class CpuAcct
{
  private static final Logger LOG = new Logger(CpuAcct.class);
  private static final String CGROUP = "cpuacct";
  private static final String CGROUP_ACCT_FILE = "cpuacct.usage_all";
  private static final long[] EMPTY_LONG = new long[0];
  private static final CpuAcctMetric EMPTY_METRIC = new CpuAcctMetric(EMPTY_LONG, EMPTY_LONG);

  public static CpuAcctMetric parse(final List<String> input)
  {
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
      return parse(java.nio.file.Files.readAllLines(cpuacct.toPath(), Charsets.UTF_8));
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
