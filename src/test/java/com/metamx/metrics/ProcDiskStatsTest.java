package com.metamx.metrics;

import java.io.File;
import java.nio.file.Files;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ProcDiskStatsTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSimple() throws Exception
  {
    final File diskStats = temporaryFolder.newFile();
    Assert.assertTrue(diskStats.delete());
    Assert.assertFalse(diskStats.exists());
    Files.copy(getClass().getResourceAsStream("/proc.diskstats"), diskStats.toPath());
    Assert.assertTrue(diskStats.exists());
    Assert.assertTrue(diskStats.isFile());
    Assert.assertTrue(diskStats.length() > 0);
    final ProcDiskStats procDiskStats = ProcDiskStats.parse(Files.readAllLines(diskStats.toPath()));
    Assert.assertEquals(21, procDiskStats.getDiskMetrics().size());
    final ProcDiskStats.DiskMetric xvdb = procDiskStats
        .getDiskMetrics()
        .stream()
        .filter((dm) -> "xvdb".equals(dm.getDevice()))
        .findFirst()
        .get();
    Assert.assertEquals("xvdb", xvdb.getDevice());
    Assert.assertEquals(17583883L, xvdb.getrComplete());
    Assert.assertEquals(184407L, xvdb.getrMerge());
    Assert.assertEquals(1366290449L, xvdb.getrSector());
    Assert.assertEquals(34760009L, xvdb.getrTimeMs());
    Assert.assertEquals(43691563L, xvdb.getwComplete());
    Assert.assertEquals(1744679L, xvdb.getwMerge());
    Assert.assertEquals(3532288298L, xvdb.getwSector());
    Assert.assertEquals(89126988L, xvdb.getwTimeMs());
    Assert.assertEquals(12L, xvdb.getActiveCount());
    Assert.assertEquals(6172146L, xvdb.getActiveTimeMs());
    Assert.assertEquals(123894931L, xvdb.getWeightedActiveTimeMs());
    final ProcDiskStats.DiskMetric delta = xvdb.deltaSince(new ProcDiskStats.DiskMetric(
        "xvdb",
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0
    ));
    Assert.assertEquals("xvdb", delta.getDevice());
    Assert.assertEquals(17583883L, delta.getrComplete());
    Assert.assertEquals(184407L, delta.getrMerge());
    Assert.assertEquals(1366290449L, delta.getrSector());
    Assert.assertEquals(34760009L, delta.getrTimeMs());
    Assert.assertEquals(43691563L, delta.getwComplete());
    Assert.assertEquals(1744679L, delta.getwMerge());
    Assert.assertEquals(3532288298L, delta.getwSector());
    Assert.assertEquals(89126988L, delta.getwTimeMs());
    Assert.assertEquals(0L, delta.getActiveCount()); // Different
    Assert.assertEquals(6172146L, delta.getActiveTimeMs());
    Assert.assertEquals(123894931L, delta.getWeightedActiveTimeMs());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailsUnequalDevice()
  {
    new ProcDiskStats.DiskMetric(
        "A",
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0
    ).deltaSince(new ProcDiskStats.DiskMetric(
        "B",
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0
    ));
  }
}
