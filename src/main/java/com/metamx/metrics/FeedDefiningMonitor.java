package com.metamx.metrics;

import com.google.common.base.Preconditions;
import com.metamx.emitter.service.ServiceMetricEvent;

public abstract class FeedDefiningMonitor extends AbstractMonitor
{
  public static final String DEFAULT_METRICS_FEED = "metrics";
  private String feed;

  public FeedDefiningMonitor(String feed)
  {
    Preconditions.checkNotNull(feed);
    this.feed = feed;
  }

  protected ServiceMetricEvent.Builder builder()
  {
    return ServiceMetricEvent.builder().setFeed(feed);
  }
}
