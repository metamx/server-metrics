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

import com.metamx.common.StringUtils;
import java.lang.management.ManagementFactory;
import java.util.regex.Pattern;

public class CgroupUtil
{
  public static final String SPACE_MATCH = Pattern.quote(" ");
  public static final String COMMA_MATCH = Pattern.quote(",");
  public static final String COLON_MATCH = Pattern.quote(":");

  /**
   * Returns the PID as a best guess. This uses methods that are not guaranteed to actually be the PID.
   *
   * TODO: switch to ProcessHandle.current().getPid() for java9 potentially
   *
   * @return the PID of the current jvm if available
   *
   * @throws IndeterminatePid if the pid cannot be determined
   */
  public static long getProbablyPID() throws IndeterminatePid
  {
    final String jvmName = ManagementFactory.getRuntimeMXBean().getName();
    final String[] nameSplits = jvmName.split(Pattern.quote("@"));
    if (nameSplits.length != 2) {
      throw new IndeterminatePid(StringUtils.safeFormat("Unable to determine pid from [%s]", jvmName));
    }
    try {
      return Long.parseLong(nameSplits[0]);
    }
    catch (NumberFormatException nfe) {
      throw new IndeterminatePid(StringUtils.safeFormat("Unable to determine pid from [%s]", jvmName), nfe);
    }
  }

  public static class IndeterminatePid extends Exception
  {
    IndeterminatePid(String reason)
    {
      super(reason);
    }

    IndeterminatePid(String message, Throwable cause)
    {
      super(message, cause);
    }
  }
}
