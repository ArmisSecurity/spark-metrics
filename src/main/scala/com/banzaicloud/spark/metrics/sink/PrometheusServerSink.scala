/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.banzaicloud.spark.metrics.sink

import com.banzaicloud.spark.metrics.DropwizardExportsWithMetricNameCaptureAndReplace
import com.codahale.metrics._
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.dropwizard.DropwizardExports
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.jmx.JmxCollector
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.sink.Sink
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}

import java.io.File
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.util.matching.Regex


class PrometheusServerSink(
                            val property: Properties,
                            val registry: MetricRegistry,
                            securityMgr: SecurityManager)
  extends Sink with Logging {

  lazy val defaultSparkConf: SparkConf = new SparkConf(true)

  val DRIVER_PORT = 9090

  lazy val server: Option[HTTPServer] = {
    if (!isDriver)
      None
    else {
      log.info("Starting Prometheus HTTP Server on port {}", DRIVER_PORT)
      Some(new HTTPServer(
        DRIVER_PORT,
        true
      ))
    }
  }

  lazy val isDriver: Boolean = {
    // SparkEnv may become available only after metrics sink creation thus retrieving
    // SparkConf from spark env here and not during the creation/initialisation of PrometheusServerSink.
    val sparkConf: SparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(defaultSparkConf)
    val executorId: Option[String] = sparkConf.getOption("spark.executor.id")
    val isDriver = executorId.contains("driver")
    isDriver
  }

  protected class Reporter(registry: MetricRegistry)
    extends ScheduledReporter(
      registry,
      "prometheus-reporter",
      MetricFilter.ALL,
      TimeUnit.SECONDS,
      TimeUnit.MILLISECONDS) {

    val defaultSparkConf: SparkConf = new SparkConf(true)

    override def report(
                         gauges: util.SortedMap[String, Gauge[_]],
                         counters: util.SortedMap[String, Counter],
                         histograms: util.SortedMap[String, Histogram],
                         meters: util.SortedMap[String, Meter],
                         timers: util.SortedMap[String, Timer]): Unit = {
    }
  }

  val DEFAULT_PERIOD: Int = 10
  val DEFAULT_PERIOD_UNIT: TimeUnit = TimeUnit.SECONDS
  val DEFAULT_PUSHGATEWAY_ADDRESS: String = "127.0.0.1:9091"
  val DEFAULT_PUSHGATEWAY_ADDRESS_PROTOCOL: String = "http"
  val PUSHGATEWAY_ENABLE_TIMESTAMP: Boolean = false

  val KEY_PERIOD = "period"
  val KEY_PERIOD_UNIT = "unit"
  val KEY_PUSHGATEWAY_ADDRESS = "pushgateway-address"
  val KEY_PUSHGATEWAY_ADDRESS_PROTOCOL = "pushgateway-address-protocol"
  val KEY_PUSHGATEWAY_ENABLE_TIMESTAMP = "pushgateway-enable-timestamp"
  val DEFAULT_KEY_JMX_COLLECTOR_CONFIG = "/opt/spark/conf/jmx_collector.yaml"

  // metrics name replacement
  val KEY_METRICS_NAME_CAPTURE_REGEX = "metrics-name-capture-regex"
  val KEY_METRICS_NAME_REPLACEMENT = "metrics-name-replacement"

  val KEY_ENABLE_DROPWIZARD_COLLECTOR = "enable-dropwizard-collector"
  val KEY_ENABLE_JMX_COLLECTOR = "enable-jmx-collector"
  val KEY_ENABLE_HOSTNAME_IN_INSTANCE = "enable-hostname-in-instance"
  val KEY_JMX_COLLECTOR_CONFIG = "jmx-collector-config"

  // labels
  val KEY_LABELS = "labels"

  val pollPeriod: Int =
    Option(property.getProperty(KEY_PERIOD))
      .map(_.toInt)
      .getOrElse(DEFAULT_PERIOD)

  val pollUnit: TimeUnit =
    Option(property.getProperty(KEY_PERIOD_UNIT))
      .map { s => TimeUnit.valueOf(s.toUpperCase) }
      .getOrElse(DEFAULT_PERIOD_UNIT)

  val metricsNameCaptureRegex: Option[Regex] =
    Option(property.getProperty(KEY_METRICS_NAME_CAPTURE_REGEX))
      .map(new Regex(_))

  val metricsNameReplacement: String =
    Option(property.getProperty(KEY_METRICS_NAME_REPLACEMENT))
      .getOrElse("")

  // validate metrics name capture regex
  if (metricsNameCaptureRegex.isDefined && metricsNameReplacement == "") {
    throw new IllegalArgumentException("Metrics name replacement must be specified if metrics name capture regexp is set !")
  }

  val enableDropwizardCollector: Boolean =
    Option(property.getProperty(KEY_ENABLE_DROPWIZARD_COLLECTOR))
      .map(_.toBoolean)
      .getOrElse(true)
  val enableJmxCollector: Boolean =
    Option(property.getProperty(KEY_ENABLE_JMX_COLLECTOR))
      .map(_.toBoolean)
      .getOrElse(false)
  val enableHostNameInInstance: Boolean =
    Option(property.getProperty(KEY_ENABLE_HOSTNAME_IN_INSTANCE))
      .map(_.toBoolean)
      .getOrElse(false)
  val jmxCollectorConfig =
    Option(property.getProperty(KEY_JMX_COLLECTOR_CONFIG))
      .getOrElse(DEFAULT_KEY_JMX_COLLECTOR_CONFIG)

  checkMinimalPollingPeriod(pollUnit, pollPeriod)

  logInfo("Initializing Prometheus Sink...")
  logInfo(s"Metrics polling period -> $pollPeriod $pollUnit")
  logInfo(s"$KEY_METRICS_NAME_CAPTURE_REGEX -> ${metricsNameCaptureRegex.getOrElse("")}")
  logInfo(s"$KEY_METRICS_NAME_REPLACEMENT -> $metricsNameReplacement")
  logInfo(s"$KEY_JMX_COLLECTOR_CONFIG -> $jmxCollectorConfig")

  val collectorRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry

  lazy val sparkMetricExports: DropwizardExports = metricsNameCaptureRegex match {
    case Some(r) => new DropwizardExportsWithMetricNameCaptureAndReplace(r, metricsNameReplacement, registry)
    case _ => new DropwizardExports(registry)
  }

  lazy val jmxMetrics: JmxCollector = new JmxCollector(new File(jmxCollectorConfig))

  val reporter = new Reporter(registry)

  override def start(): Unit = {
    if (enableDropwizardCollector) {
      sparkMetricExports.register(collectorRegistry)
    }
    if (enableJmxCollector) {
      jmxMetrics.register(collectorRegistry)
    }
    reporter.start(pollPeriod, pollUnit)
    // maybe start the http server
    server
  }

  override def stop(): Unit = {
    reporter.stop()
    if (enableDropwizardCollector) {
      collectorRegistry.unregister(sparkMetricExports)
    }
    if (enableJmxCollector) {
      collectorRegistry.unregister(jmxMetrics)
    }
    server.foreach(_.stop())
  }

  override def report(): Unit = {
    reporter.report()
  }

  private def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int) {
    val period = TimeUnit.SECONDS.convert(pollPeriod, pollUnit)
    if (period < 1) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
        " below than minimal polling period ")
    }
  }
}
