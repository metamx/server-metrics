package com.metamx.metrics;

import org.junit.Assert;
import org.junit.Test;

public class CgroupUtilTest
{
  @Test
  public void testSimple() throws Exception
  {
    CgroupUtil.getProbablyPID();
  }
}