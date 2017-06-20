package com.metamx.metrics;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.StringUtils;
import com.metamx.metrics.cgroups.CgroupDiscoverer;
import com.metamx.metrics.cgroups.PidDiscoverer;
import com.metamx.metrics.cgroups.ProcCgroupDiscoverer;
import com.metamx.metrics.cgroups.TestUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class CpuAcctDeltaMonitorTest
{
  private static final int PID = 384;
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File procDir;
  private File cgroupDir;
  private File cpuacctDir;
  private CgroupDiscoverer discoverer;

  @Before
  public void setUp() throws IOException
  {
    cgroupDir = temporaryFolder.newFolder();
    procDir = temporaryFolder.newFolder();
    discoverer = new ProcCgroupDiscoverer()
    {
      @Override
      public File proc()
      {
        return procDir;
      }

      @Override
      public long getProbabyPid()
      {
        return PID;
      }
    };
    TestUtils.setUpCgroups(procDir, cgroupDir, PID);
    cpuacctDir = new File(
        cgroupDir,
        "cpu,cpuacct/system.slice/mesos-agent-druid.service/f12ba7e0-fa16-462e-bb9d-652ccc27f0ee"
    );
    Assert.assertTrue((cpuacctDir.isDirectory() && cpuacctDir.exists()) || cpuacctDir.mkdirs());
    TestUtils.copyResource("/cpuacct.usage_all", new File(cpuacctDir, "cpuacct.usage_all"));
  }

  @Test
  public void testSimpleMonitor() throws Exception
  {
    final File cpuacct = new File(cpuacctDir, "cpuacct.usage_all");
    try (final FileOutputStream fos = new FileOutputStream(cpuacct)) {
      fos.write(StringUtils.toUtf8("cpu user system\n"));
      for (int i = 0; i < 128; ++i) {
        fos.write(StringUtils.toUtf8(String.format("%d 0 0\n", i)));
      }
    }
    final CpuAcctDeltaMonitor monitor = new CpuAcctDeltaMonitor("some_feed", ImmutableMap.of(), new PidDiscoverer()
    {
      @Override
      public long getPid()
      {
        return PID;
      }
    }, (cgroup, pid) -> cpuacctDir.toPath());
    final MonitorsTest.StubServiceEmitter emitter = new MonitorsTest.StubServiceEmitter("service", "host");
    Assert.assertFalse(monitor.doMonitor(emitter));
    // First should just cache
    Assert.assertEquals(0, emitter.getEvents().size());
    Assert.assertTrue(cpuacct.delete());
    TestUtils.copyResource("/cpuacct.usage_all", cpuacct);
    Assert.assertTrue(monitor.doMonitor(emitter));
    Assert.assertEquals(2 * 128 + 1, emitter.getEvents().size());
  }
}
