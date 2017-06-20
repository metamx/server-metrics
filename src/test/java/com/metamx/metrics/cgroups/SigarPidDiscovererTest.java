package com.metamx.metrics.cgroups;

import org.junit.Assert;
import org.junit.Test;

public class SigarPidDiscovererTest
{
  @Test
  public void simpleTest() {
    Assert.assertNotNull(new SigarPidDiscoverer().getPid());
  }
}