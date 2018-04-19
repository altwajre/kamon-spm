/*
 * =========================================================================================
 * Copyright © 2013-2018 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package kamon.spm

import java.io.ByteArrayOutputStream
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.time.Duration
import java.util
import java.util.{Locale, Properties}

import com.typesafe.config.Config
import kamon.metric.MeasurementUnit.Dimension.Information
import kamon.metric.MeasurementUnit.{information, time}
import kamon.metric.{MeasurementUnit, _}
import kamon.{Kamon, MetricReporter}
import org.asynchttpclient.util.ProxyUtils
import org.asynchttpclient._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.Map
import scala.concurrent.{Future, blocking}
import spray.json._
import DefaultJsonProtocol._
import com.sematext.spm.client.tracing.thrift._

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.Random


class SPMReporter extends MetricReporter {
  private val log = LoggerFactory.getLogger(classOf[SPMReporter])
  val config = Kamon.config().getConfig("kamon.spm")
  val maxQueueSize = config.getInt("max-queue-size")
  val retryInterval = config.getDuration("retry-interval")
  val url = config.getString("receiver-url")
  val tracingUrl = config.getString("tracing-receiver-url")
  val token = config.getString("token")
  val customMarker = config.getString("custom-metric-marker")
  val traceDurationThreshold = config.getString("trace-duration-threshhold").toLong
  private val IndexTypeHeader = Map("index" -> Map("_type" -> "log", "_index" -> "spm-receiver"))
  val host = if (config.hasPath("hostname-alias")) {
    config.getString("hostname-alias")
  } else {
    InetAddress.getLocalHost.getHostName
  }
  var httpClient: AsyncHttpClient = null;
  var DEFAULT_DIM = "default"

  override def start(): Unit = {
    log.info("Starting kamon-spm reporter")
    val sendTimeout = config.getDuration("send-timeout")
    val proxy = System.getProperty("http.proxyHost")
    val proxyProps = new Properties()
    if (proxy != null && !proxy.isEmpty) {
      val proxyPort = System.getProperty("http.proxyPort")
      if (proxyPort == null || proxyPort.isEmpty) {
        log.error(s"Proxy port not specified")
      } else {
        proxyProps.setProperty(ProxyUtils.PROXY_HOST, proxy)
        proxyProps.setProperty(ProxyUtils.PROXY_PORT, proxyPort)
        val proxyUser = System.getProperty("http.proxyUser")
        val proxyPassword = System.getProperty("http.proxyPassword")
        proxyProps.setProperty(ProxyUtils.PROXY_USER, if (proxyUser == null) "" else proxyUser)
        proxyProps.setProperty(ProxyUtils.PROXY_PASSWORD, if (proxyPassword == null) "" else proxyPassword)
      }
    } else {
      val proxy = config.getString("proxy-server")
      if (proxy != null && !proxy.isEmpty) {
        proxyProps.setProperty(ProxyUtils.PROXY_HOST, proxy)
        proxyProps.setProperty(ProxyUtils.PROXY_PORT, config.getInt("proxy-port").toString)
        proxyProps.setProperty(ProxyUtils.PROXY_USER, config.getString("proxy-user"))
        proxyProps.setProperty(ProxyUtils.PROXY_PASSWORD, config.getString("proxy-password"))
      }
    }
    httpClient = new AsyncHttpClient(sendTimeout.toMillis.toInt, log, proxyProps)
  }

  override def stop(): Unit = {
    if (httpClient != null) {
      httpClient.close()
    }
  }

  override def reconfigure(config: Config): Unit = {
  }


  override def reportPeriodSnapshot(snapshot: PeriodSnapshot): Unit = {
    val metrics = snapshot.metrics
    try {
      postTraces(buildTraceRequestBody(snapshot))
    } catch {
      case e: Throwable ⇒ {
        log.error("Something went wrong while trace metrics sending.", e)
      }
    }
    try {
      post(buildRequestBody(snapshot))
    } catch {
      case e: Throwable ⇒ {
        log.error("Something went wrong.", e)
      }
    }
  }

  private def addMetric(dimension: String, metrics: String)(processedMetrics: scala.collection.mutable.Map[String, String]) {
    if (processedMetrics.contains(dimension)) {
      val st = processedMetrics.get(dimension)
      processedMetrics.put(dimension, st.get.concat(",").concat(metrics))
    } else {
      processedMetrics.put(dimension, metrics)
    }
  }

  private def buildTraceRequestBody(snapshot: PeriodSnapshot): Array[Byte] = {
    val timestamp: Long = (snapshot.from.toEpochMilli)
    val baos = new ByteArrayOutputStream()
    val histograms = (snapshot.metrics.histograms.filter(metric ⇒ (metric.name == "span.processing-time" && metric.distribution.max > traceDurationThreshold))).foreach { metric ⇒
      val event = new TTracingEvent()
      val thrift = new TPartialTransaction()
      val rnd = new Random()
      val callId = rnd.nextLong()
      thrift.setParentCallId(0)
      thrift.setCallId(callId)
      thrift.setTraceId(rnd.nextLong())
      val calls = new java.util.ArrayList[TCall]()
      val tCall = new TCall()
      tCall.setDuration(convert(metric.unit, metric.distribution.max))
      tCall.setStartTimestamp(timestamp)
      tCall.setEndTimestamp(timestamp + convert(metric.unit, metric.distribution.max))
      tCall.setCallId(callId)
      tCall.setParentCallId(0)
      tCall.setSignature(metric.tags("operation"))
      calls.add(tCall)
      //TODO: implement trace sections
      thrift.setRequest(metric.tags("operation"))
      thrift.setStartTimestamp(timestamp)
      thrift.setEndTimestamp(timestamp + convert(metric.unit, metric.distribution.max))
      thrift.setDuration(convert(metric.unit, metric.distribution.max))
      thrift.setToken(token)
      thrift.setFailed(metric.tags("error").toBoolean)
      thrift.setEntryPoint(true)
      thrift.setAsynchronous(false)
      thrift.setTransactionType(TTransactionType.WEB)
      val summary = new TWebTransactionSummary()
      summary.setRequest(metric.tags("operation"))
      thrift.setTransactionSummary(ThriftUtils.binaryProtocolSerializer().serialize(summary))
      val endpoint = new TEndpoint()
      endpoint.setHostname(InetAddress.getLocalHost.getHostName)
      endpoint.setAddress(InetAddress.getLocalHost.getHostAddress)
      thrift.setEndpoint(endpoint)
      thrift.setCalls(calls)
      thrift.setParameters(new java.util.HashMap[String, String]())
      event.setPartialTransaction(thrift)
      event.eventType = TTracingEventType.PARTIAL_TRANSACTION
      val trace = ThriftUtils.binaryProtocolSerializer().serialize(event)
      baos.write(ByteBuffer.allocate(4).putInt(trace.size).array())
      baos.write(trace)
    }
    baos.toByteArray
  }

  private def buildRequestBody(snapshot: PeriodSnapshot): Array[Byte] = {
    val timestamp: Long = (snapshot.from.toEpochMilli)
    val processedMetrics = scala.collection.mutable.Map[String, String]()
    (snapshot.metrics.histograms ++ snapshot.metrics.rangeSamplers).map { metric ⇒ buildHistograms(metric, processedMetrics)}
    val counters = (snapshot.metrics.counters).map { metric ⇒ val metrisString = buildCounters(metric, processedMetrics)}
    val gauges = (snapshot.metrics.gauges).map { metric ⇒ val metrisString = buildGauges(metric, processedMetrics)}
    val requestData = processedMetrics.view.map { case (k, v) =>
      if (DEFAULT_DIM.equals(k)) {
        prefix().concat(" ").concat(v).concat(" ").concat(timestamp.toString).concat("000000").trim.replaceAll(" +", " ")
      } else {
        prefix().concat(",").concat(k).concat(" ").concat(v).concat(" ").concat(timestamp.toString).concat("000000").trim.replaceAll(" +", " ")
      }
    }.toList
    (requestData).mkString("\n").getBytes(StandardCharsets.UTF_8)
  }

  private def buildCounters(metric: MetricValue, processedMetrics: scala.collection.mutable.Map[String, String]) = {
    metric.name match {
      case "host.file-system.activity" => addMetric(DEFAULT_DIM, s"os.disk.io.${getTagOrEmptyString(metric.tags, "operation")}=${metric.value}")(processedMetrics)
      case "akka.actor.errors" => addMetric(s"actor=${getTagOrEmptyString(metric.tags, "path")}", s"actor.errors=${metric.value}")(processedMetrics)
      case "akka.router.errors" => addMetric(DEFAULT_DIM, s"router.processing.errors=${metric.value}")(processedMetrics)
      case "executor.tasks" => addMetric(DEFAULT_DIM, "dispatcher.executor.tasks.processed=${metric.value}")(processedMetrics)
      case "host.network.packets" => {
        if (metric.tags.contains("state")) {
          getTagOrEmptyString(metric.tags, "state") match {
            case "error" => {
              getTagOrEmptyString(metric.tags, "direction") match {
                case "transmitted" => addMetric(DEFAULT_DIM, s"os.network.tx.errors.sum=${metric.value},os.network.tx.errors.count=1")(processedMetrics)
                case "received" => addMetric(DEFAULT_DIM, s"os.network.rx.errors.sum=${metric.value},os.network.rx.errors.count=1")(processedMetrics)
                case _ => ""
              }
            }
            case "dropped" => {
              getTagOrEmptyString(metric.tags, "direction") match {
                case "transmitted" => addMetric(DEFAULT_DIM, s"os.network.tx.dropped.sum=${metric.value},os.network.tx.dropped.count=1")(processedMetrics)
                case "received" => addMetric(DEFAULT_DIM, s"os.network.rx.dropped.sum=${metric.value},os.network.rx.dropped.count=1")(processedMetrics)
                case _ => ""
              }
            }
            case _ => ""
          }
        } else {
          getTagOrEmptyString(metric.tags, "direction") match {
            case "transmitted" => addMetric(DEFAULT_DIM, s"os.network.tx.rate=${metric.value}")(processedMetrics)
            case "received" => addMetric(DEFAULT_DIM, s"os.network.rx.rate=${metric.value}")(processedMetrics)
            case _ => ""
          }
        }
      }
      case _ => {
        if (metric.tags.contains(customMarker)) {
          addMetric(s"metric=counter-counter", s"custom.counter.count=${metric.value}")(processedMetrics)
        } else {
          s""
        }
      }
    }
  }

  private def buildGauges(metric: MetricValue, processedMetrics: scala.collection.mutable.Map[String, String]) = {
    metric.name match {
      case "executor.pool" => getTagOrEmptyString(metric.tags, "setting") match {
        case "parallelism" => addMetric(s"dispatcher=${getTagOrEmptyString(metric.tags, "name")}", s"dispatcher.fj.parallelism=${convert(metric.unit, metric.value)}")(processedMetrics)
        case "min" => addMetric(s"dispatcher=${getTagOrEmptyString(metric.tags, "name")}", s"dispatcher.executor.pool=${convert(metric.unit, metric.value)}")(processedMetrics)
        case "max" => addMetric(s"dispatcher=${getTagOrEmptyString(metric.tags, "name")}", s"dispatcher.executor.pool.max=${convert(metric.unit, metric.value)}")(processedMetrics)
        case "corePoolSize" => addMetric(s"dispatcher=${getTagOrEmptyString(metric.tags, "name")}", s"dispatcher.executor.pool.core=${convert(metric.unit, metric.value)}")(processedMetrics)
        case _ => ""
      }
      case "jvm.class-loading" => {
        getTagOrEmptyString(metric.tags, "mode") match {
          case "currently-loaded" => addMetric(DEFAULT_DIM, s"jvm.classes.loaded=${metric.value}")(processedMetrics)
          case "unloaded" => addMetric(DEFAULT_DIM, s"jvm.classes.unloaded=${metric.value}")(processedMetrics)
          case "loaded" => addMetric(DEFAULT_DIM, s"jvm.classes.loaded.total=${metric.value}")(processedMetrics)
          case _ => ""
        }
      }
      case "jvm.threads" => getTagOrEmptyString(metric.tags, "measure") match {
        case "daemon" => addMetric(DEFAULT_DIM, s"jvm.threads.deamon.sum=${metric.value},jvm.threads.deamon.count=1")(processedMetrics)
        case "peak" => addMetric(DEFAULT_DIM, s"jvm.threads.max.sum=${metric.value},jvm.threads.max.count=1")(processedMetrics)
        case "total" => addMetric(DEFAULT_DIM, s"jvm.threads.sum=${metric.value},jvm.threads.count=1")(processedMetrics)
        case _ => ""
      }
      case _ => {
        if (metric.tags.contains(customMarker)) {
          addMetric("metric=counter-counter", s"custom.counter.sum=${metric.value}")(processedMetrics)
        } else {
          s""
        }
      }
    }
  }

  private def buildHistograms(metric: MetricDistribution, processedMetrics: scala.collection.mutable.Map[String, String]) = {
    val min = convert(metric.unit, metric.distribution.min)
    val max = convert(metric.unit, metric.distribution.max)
    val sum = convert(metric.unit, metric.distribution.sum)
    val count = convert(metric.unit, metric.distribution.count);
    val p50 = convert(metric.unit, metric.distribution.percentile(50D).value)
    val p90 = convert(metric.unit, metric.distribution.percentile(90D).value)
    val p95 = convert(metric.unit, metric.distribution.percentile(95D).value)
    val p99 = convert(metric.unit, metric.distribution.percentile(99D).value)
    val p995 = convert(metric.unit, metric.distribution.percentile(99.5D).value)


    metric.name match {
      case "host.cpu" => addMetric(DEFAULT_DIM, s"os.cpu.${getTagOrEmptyString(metric.tags, "mode")}.sum=${sum},os.cpu.${getTagOrEmptyString(metric.tags, "mode")}.count=${count}")(processedMetrics)
      case "host.load-average" => addMetric(DEFAULT_DIM, s"os.load.${getTagOrEmptyString(metric.tags, "period")}min.sum=${sum},os.load.${getTagOrEmptyString(metric.tags, "period")}min.count=${count}")(processedMetrics)

      case "host.swap" => addMetric(DEFAULT_DIM, s"os.swap.${getTagOrEmptyString(metric.tags, "mode")}.sum=${sum},os.swap.${{getTagOrEmptyString(metric.tags, "mode")}}.count=${count}")(processedMetrics)
      case "host.memory" => addMetric(DEFAULT_DIM, s"os.memory.${getTagOrEmptyString(metric.tags, "mode")}.sum=${sum},os.memory.${{getTagOrEmptyString(metric.tags, "mode")}}.count=${count}")(processedMetrics)

      case "akka.actor.time-in-mailbox" => addMetric(s"actor=${getTagOrEmptyString(metric.tags, "path")}", s"actor.mailbox.time.min=${min},actor.mailbox.time.max=${max},actor.mailbox.time.sum=${sum},actor.mailbox.time.count=${count}")(processedMetrics)
      case "akka.actor.processing-time" => addMetric(s"actor=${getTagOrEmptyString(metric.tags, "path")}", s"actor.processing.time.min=${min},actor.processing.time.max=${max},actor.processing.time.sum=${sum},actor.processing.time.count=${count}")(processedMetrics)
      case "akka.actor.mailbox-size" => addMetric(s"actor=${getTagOrEmptyString(metric.tags, "path")}", s"actor.mailbox.size.sum=${sum},actor.mailbox.size.count=${count}")(processedMetrics)

      case "akka.router.routing-time" =>  addMetric(s"router=${getTagOrEmptyString(metric.tags, "name")}", s"router.routing.time.min=${min},router.routing.time.max=${max},router.routing.time.sum=${sum},router.routing.time.count=${count}")(processedMetrics)
      case "akka.router.time-in-mailbox" =>  addMetric(s"router=${getTagOrEmptyString(metric.tags, "name")}", s"router.mailbox.time.min=${min},router.mailbox.time.max=${max},router.mailbox.time.sum=${sum},router.mailbox.time.count=${count}")(processedMetrics)
      case "akka.router.processing-time" =>  addMetric(s"router=${getTagOrEmptyString(metric.tags, "name")}", s"router.processing.time.min=${min},router.processing.time.max=${max},router.processing.time.sum=${sum},router.processing.time.count=${count}")(processedMetrics)

      case "executor.queue" => addMetric(DEFAULT_DIM, s"dispatcher.fj.tasks.queued=${max}")(processedMetrics)

      case "executor.threads" => {
        getTagOrEmptyString(metric.tags, "state") match {
          case "total" => addMetric(DEFAULT_DIM, s"dispatcher.fj.threads.running=${max}")(processedMetrics)
          case "active" => addMetric(DEFAULT_DIM, s"dispatcher.executor.threads.active=${max}")(processedMetrics)
          case _ => ""
        }
      }
      case "jvm.gc" => addMetric(s"gc=${getTagOrEmptyString(metric.tags, "collector")}", s"jvm.gc.collection.time=${sum}")(processedMetrics)
      case "jvm.gc.promotion" => addMetric(s"gc=${getTagOrEmptyString(metric.tags, "space")}", s"jvm.gc.collection.count=${sum}")(processedMetrics)

      case "jvm.memory" => addMetric(DEFAULT_DIM, s"jvm.${getTagOrEmptyString(metric.tags, "segment")}.${getTagOrEmptyString(metric.tags, "measure")}.sum=${sum},jvm.${getTagOrEmptyString(metric.tags, "segment")}.${getTagOrEmptyString(metric.tags, "measure")}.count=${count}")(processedMetrics)
      case "span.processing-time" => {
        getTagOrEmptyString(metric.tags, "error") match {
          case "false" => addMetric(s"trace=${getTagOrEmptyString(metric.tags, "operation")}", s"tracing.requests.time.min=${min},tracing.requests.time.max=${max},tracing.requests.time.sum=${sum},tracing.requests.time.count=${count}")(processedMetrics)
          case "true" => addMetric(s"trace=${getTagOrEmptyString(metric.tags, "operation")}", s"tracing.requests.errors=${count}")(processedMetrics)
          case _ => ""
        }
      }
      case "process.cpu" => addMetric(DEFAULT_DIM, s"process.cpu.${getTagOrEmptyString(metric.tags, "mode")}.count=${count},process.cpu.${getTagOrEmptyString(metric.tags, "mode")}.sum=${sum}")(processedMetrics)
      case _ => {
        if (metric.tags.contains(customMarker)) {
          addMetric("metric=histogram-histogram", s"custom.histogram.min=${min},custom.histogram.max=${max},custom.histogram.sum=${sum},custom.histogram.count=${count},custom.histogram.p50=${p50},custom.histogram.p90=${p90},custom.histogram.p99=${p99}")(processedMetrics)
        } else {
          ""
        }
      }

    }
  }

  private def prefix(): String = {
    s"akka,token=${token},os.host=${host}"
  }

  private def convert(unit: MeasurementUnit, value: Long): Long = unit.dimension.name match {
    case "time" ⇒ MeasurementUnit.scale(value, unit, time.milliseconds).toLong
    case "information" ⇒ MeasurementUnit.scale(value, unit, information.bytes).toLong
    case _ ⇒ value
  }

  trait HttpClient {
    def post(uri: String, payload: Array[Byte]): Future[Response]
    def close()
  }

  class AsyncHttpClient(sendTimeout: Int, logger: Logger, proxyProperties: Properties) extends HttpClient {
    val cf = new DefaultAsyncHttpClientConfig.Builder().setRequestTimeout(sendTimeout)
    if (!proxyProperties.isEmpty) {
      val proxySelector = ProxyUtils.createProxyServerSelector(proxyProperties)
      cf.setProxyServerSelector(proxySelector)
    }
    val aclient = new DefaultAsyncHttpClient(cf.build())

    import scala.concurrent.ExecutionContext.Implicits.global

    override def post(uri: String, payload: Array[Byte]) = {
      val javaFuture = aclient.preparePost(uri).setBody(payload).execute(
        new AsyncCompletionHandler[Response] {
          override def onCompleted(response: Response): Response = {
            logger.debug(s"${response.getStatusCode} ${response.getStatusText}")
            response
          }

          override def onThrowable(t: Throwable): Unit =
            logger.error(s"Unable to send metrics to SPM: ${t.getMessage}", t)
        })
      Future {
        blocking {
          javaFuture.get
        }
      }
    }
    override def close() = {
      aclient.close()
    }

  }

  private def generateQueryString(queryMap: Map[String, String]): String = {
    queryMap.map { case (key, value) ⇒ s"$key=$value" } match {
      case Nil ⇒ ""
      case xs ⇒ s"?${xs.mkString("&")}"
    }
  }

  private def post(body: Array[Byte]): Unit = {
    val queryString = generateQueryString(Map("host" -> host, "token" -> token))
    httpClient.post(s"$url", body).recover {
      case t: Throwable ⇒ {
        log.error("Can't post metrics.", t)
      }
    }
  }

  private def postTraces(body: Array[Byte]): Unit = {
    if (body.length == 0) return;
    val queryString = generateQueryString(Map("host" -> host, "token" -> token))
    httpClient.post(s"$tracingUrl$queryString", body).recover {
      case t: Throwable ⇒ {
        log.error("Can't post trace metrics.", t)
      }
    }
  }

  private def getTagOrEmptyString(tags: Map[String, String], tagname: String): String = {
    if (tags.contains(tagname)) {
      tags.get(tagname).get
    } else {
      ""
    }
  }
}

