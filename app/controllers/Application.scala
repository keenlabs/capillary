package controllers

import java.io.StringWriter
import java.util.concurrent.TimeUnit

import com.codahale.metrics.json.MetricsModule
import com.fasterxml.jackson.databind.{ObjectMapper, ObjectWriter}
import com.yammer.metrics.reporting.DatadogReporter
import models.{KafkaMessageReader, Metrics, ZkKafka}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import play.api.Play.current
import play.api._
import play.api.mvc._

import scala.language.implicitConversions

object Application extends Controller {

  val validUnits = Some(Set("NANOSECONDS", "MICROSECONDS", "MILLISECONDS", "SECONDS", "MINUTES", "HOURS", "DAYS"))
  val mapper = new ObjectMapper()

  def registryName: String = Play.configuration.getString("capillary.metrics.name").getOrElse("default")

  def rateUnit: String = Play.configuration.getString("capillary.metrics.rateUnit", validUnits).getOrElse("SECONDS")

  def durationUnit: String = Play.configuration.getString("capillary.metrics.durationUnit", validUnits).getOrElse("SECONDS")

  def showSamples: Boolean = Play.configuration.getBoolean("capillary.metrics.showSamples").getOrElse(false)

  def ddAPIKey: Option[String] = Play.configuration.getString("capillary.metrics.datadog.apiKey")

  val module = new MetricsModule(rateUnit, durationUnit, showSamples)
  mapper.registerModule(module)

  ddAPIKey match {
    case Some(apiKey) =>
      Logger.info("Starting Datadog Reporter")
      val reporter = new DatadogReporter.Builder()
        .withApiKey(apiKey)
        // .withMetricNameFormatter(ShortenedNameFormatter)
        .build()
      val period = 20L
      reporter.start(period, TimeUnit.SECONDS)
    case _ => Unit
  }

  implicit def stringToTimeUnit(s: String): TimeUnit = TimeUnit.valueOf(s)

  def index: Action[AnyContent] = Action { implicit request =>

    val topos = ZkKafka.getTopologies

    Ok(views.html.index(topos))
  }

  def topo(name: String, topoRoot: String, topic: String): Action[AnyContent] = Action { implicit request =>
    val totalsAndDeltas = ZkKafka.getTopologyDeltas(topoRoot, topic)
    val dateFormat = DateTimeFormat.fullDateTime()
    val generatedAt = new DateTime().toString(dateFormat)
    Ok(views.html.topology(name, topic, totalsAndDeltas._1, totalsAndDeltas._2, generatedAt))
  }

  def message(topic: String, partition: Int, offset: Long): Action[AnyContent] = Action { implicit request =>
    val content = KafkaMessageReader.getMessageContent(topic, partition, offset)
    Ok(views.html.messageAtOffset(topic, partition, offset, content))
  }

  def metrics: Action[AnyContent] = Action {
    val writer: ObjectWriter = mapper.writerWithDefaultPrettyPrinter()
    val stringWriter = new StringWriter()
    writer.writeValue(stringWriter, Metrics.metricRegistry)
    Ok(stringWriter.toString).as("application/json").withHeaders("Cache-Control" -> "must-revalidate,no-cache,no-store")
  }
}
