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
