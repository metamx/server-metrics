package com.metamx.metrics.cgroups;

import com.google.common.base.Throwables;
import com.metamx.metrics.CgroupUtil;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InterningJvmPidDiscoverer implements PidDiscoverer
{
  private static final String KEY = "pid";
  private final ConcurrentMap<String, Long> pid = new ConcurrentHashMap<>();

  @Override
  public long getPid()
  {
    return pid.computeIfAbsent(KEY, unused -> {
      try {
        return CgroupUtil.getProbablyPID();
      }
      catch (CgroupUtil.IndeterminatePid indeterminatePid) {
        throw Throwables.propagate(indeterminatePid);
      }
    });
  }
}
