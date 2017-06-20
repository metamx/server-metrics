package com.metamx.metrics.cgroups;

import com.metamx.common.StringUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.regex.Pattern;
import org.junit.Assert;

public class TestUtils
{
  public static void setUpCgroups(
      File procDir,
      File cgroupDir,
      int pid
  ) throws IOException
  {
    final File procMountsTemplate = new File(procDir, "mounts.template");
    final File procMounts = new File(procDir, "mounts");
    copyResource("/proc.mounts", procMountsTemplate);

    final String procMountsString = StringUtils.fromUtf8(Files.readAllBytes(procMountsTemplate.toPath()));
    Files.write(
        procMounts.toPath(),
        StringUtils.toUtf8(procMountsString.replaceAll(
            Pattern.quote("/sys/fs/cgroup"),
            cgroupDir.getAbsolutePath()
        ))
    );

    Assert.assertTrue(new File(
        cgroupDir,
        "cpu,cpuacct/system.slice/mesos-agent-druid.service/f12ba7e0-fa16-462e-bb9d-652ccc27f0ee"
    ).mkdirs());

    copyResource("/proc.cgroups", new File(procDir, "cgroups"));
    final File pidDir = new File(procDir, Integer.toString(pid));
    Assert.assertTrue(pidDir.mkdir());
    copyResource("/proc.pid.cgroup", new File(pidDir, "cgroup"));
  }

  public static void copyResource(String resource, File out) throws IOException
  {
    Files.copy(TestUtils.class.getResourceAsStream(resource), out.toPath());
    Assert.assertTrue(out.exists());
    Assert.assertNotEquals(0, out.length());
  }
}
