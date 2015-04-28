package com.metamx.metrics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.service.ServiceMetricEvent;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class MonitorUtilsTest
{
  @Test
  public void testAddDimensionsToBuilder()
  {
    ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    Map<String, String[]> dimensions = ImmutableMap.of(
        "dim1", new String[]{"value1"},
        "dim2", new String[]{"value2.1", "value2.2"}
    );

    MonitorUtils.addDimensionsToBuilder(builder, dimensions);

    Assert.assertEquals(builder.getDimension("dim1"), ImmutableList.of("value1"));
    Assert.assertEquals(builder.getDimension("dim2"), ImmutableList.of("value2.1", "value2.2"));
  }
}
