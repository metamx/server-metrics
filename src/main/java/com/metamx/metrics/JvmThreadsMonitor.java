package com.metamx.metrics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import java.util.Map;
import org.gridkit.lab.jvm.perfdata.JStatData;
import org.gridkit.lab.jvm.perfdata.JStatData.LongCounter;

public class JvmThreadsMonitor extends AbstractMonitor
{
  private final Map<String, String[]> dimensions;
  private final LongCounter deamonCounter;
  private final LongCounter liveCounter;
  private final LongCounter livePeakCounter;
  private final LongCounter startedCounter;

  private long lastStartedThreads = 0;

  public JvmThreadsMonitor()
  {
    this(ImmutableMap.<String, String[]>of());
  }

  public JvmThreadsMonitor(Map<String, String[]> dimensions)
  {
    Preconditions.checkNotNull(dimensions);
    this.dimensions = ImmutableMap.copyOf(dimensions);

    long currentProcessId = SigarUtil.getCurrentProcessId();
    // connect to itself
    JStatData jStatData = JStatData.connect(currentProcessId);
    Map<String, JStatData.Counter<?>> jStatCounters = jStatData.getAllCounters();

    deamonCounter = (LongCounter) jStatCounters.get("java.threads.daemon");
    liveCounter = (LongCounter) jStatCounters.get("java.threads.live");
    livePeakCounter = (LongCounter) jStatCounters.get("java.threads.livePeak");
    startedCounter = (LongCounter) jStatCounters.get("java.threads.started");
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    MonitorUtils.addDimensionsToBuilder(builder, dimensions);

    emitter.emit(builder.build("jvm/threads/daemon", deamonCounter.getLong()));
    emitter.emit(builder.build("jvm/threads/live", liveCounter.getLong()));
    emitter.emit(builder.build("jvm/threads/livePeak", livePeakCounter.getLong()));

    long newStartedThreads = startedCounter.getLong();
    emitter.emit(builder.build("jvm/threads/started", newStartedThreads - lastStartedThreads));
    lastStartedThreads = newStartedThreads;

    return true;
  }
}
