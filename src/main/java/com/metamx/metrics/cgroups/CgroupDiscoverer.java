package com.metamx.metrics.cgroups;

import java.nio.file.Path;

public interface CgroupDiscoverer
{
  Path discover(String cgroup, long pid);
}
