package utils

import models.Metrics
import nl.grons.metrics.scala.InstrumentedBuilder

trait Instrumented extends InstrumentedBuilder {
  val metricRegistry = Metrics.metricRegistry
}
