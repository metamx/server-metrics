package com.metamx.metrics.cgroups;

import org.junit.Assert;
import org.junit.Test;

public class InterningJvmPidDiscovererTest
{
  @Test
  public void getPid() throws Exception
  {
    Assert.assertNotNull(new InterningJvmPidDiscoverer().getPid());
  }
}
