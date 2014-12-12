package models

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jvm._

object Metrics {
  val metricRegistry = new MetricRegistry()
  metricRegistry.registerAll(new GarbageCollectorMetricSet())
  metricRegistry.registerAll(new MemoryUsageGaugeSet())
  metricRegistry.registerAll(new ThreadStatesGaugeSet())
  metricRegistry.register("fd.usage", new FileDescriptorRatioGauge())
}