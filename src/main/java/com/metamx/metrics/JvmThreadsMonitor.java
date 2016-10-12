package com.metamx.metrics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Map;
import org.gridkit.lab.jvm.perfdata.JStatData;
import org.gridkit.lab.jvm.perfdata.JStatData.LongCounter;

public class JvmThreadsMonitor extends AbstractMonitor
{
  private final Map<String, String[]> dimensions;

  private int lastLiveThreads = 0;
  private long lastStartedThreads = 0;

  public JvmThreadsMonitor()
  {
    this(ImmutableMap.<String, String[]>of());
  }

  public JvmThreadsMonitor(Map<String, String[]> dimensions)
  {
    Preconditions.checkNotNull(dimensions);
    this.dimensions = ImmutableMap.copyOf(dimensions);
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    MonitorUtils.addDimensionsToBuilder(builder, dimensions);

    int newLiveThreads = threadBean.getThreadCount();
    long newStartedThreads = threadBean.getTotalStartedThreadCount();

    long startedThreadsDiff = newStartedThreads - lastStartedThreads;

    emitter.emit(builder.build("jvm/threads/daemon", threadBean.getDaemonThreadCount()));
    emitter.emit(builder.build("jvm/threads/livePeak", threadBean.getPeakThreadCount()));
    emitter.emit(builder.build("jvm/threads/live", newLiveThreads));
    emitter.emit(builder.build("jvm/threads/started", startedThreadsDiff));
    emitter.emit(builder.build("jvm/threads/finished", lastLiveThreads + startedThreadsDiff - newLiveThreads));

    threadBean.resetPeakThreadCount();

    lastStartedThreads = newStartedThreads;
    lastLiveThreads = newLiveThreads;

    return true;
  }
}
