package models

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jvm.{ThreadStatesGaugeSet, GarbageCollectorMetricSet, MemoryUsageGaugeSet}

object Metrics {
  val metricRegistry = new MetricRegistry()
  metricRegistry.registerAll(new GarbageCollectorMetricSet())
  metricRegistry.registerAll(new MemoryUsageGaugeSet())
  metricRegistry.registerAll(new ThreadStatesGaugeSet())
}