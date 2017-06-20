package com.metamx.metrics.cgroups;

import com.metamx.metrics.SigarUtil;
import org.hyperic.sigar.Sigar;

public class SigarPidDiscoverer implements PidDiscoverer
{
  private final Sigar sigar = SigarUtil.getSigar();

  @Override
  public long getPid()
  {
    return sigar.getPid();
  }
}
