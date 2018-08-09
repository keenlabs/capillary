package models

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import play.api.{Logger, Play}
import play.api.Play.current
import play.api.libs.json._
import scala.collection.JavaConverters._

object ZkKafka {

  case class Totals(total: Long, kafkaTotal: Long, spoutTotal: Long)

  case class Delta(partition: Int, amount: Option[Long], current: Long, storm: Option[Long])

  case class Topology(name: String, spoutRoot: String, topic: String)

  def topoCompFn(t1: Topology, t2: Topology): Boolean = {
    (t1.name compareToIgnoreCase t2.name) < 0
  }

  val zookeepers = Play.configuration.getString("capillary.zookeepers").getOrElse("localhost:2181")
  val kafkaZookeepers = Play.configuration.getString("capillary.kafka.zookeepers").getOrElse(zookeepers)
  val stormZookeepers = Play.configuration.getString("capillary.storm.zookeepers").getOrElse(zookeepers)

  // converts the empty string to None
  private def getConfigStringHandleEmpty(configName: String): Option[String] = Play.configuration.getString(configName) match {
    case Some("") => None
    case x => x
  }

  val kafkaZkRoot = getConfigStringHandleEmpty("capillary.kafka.zkroot")
  val stormZkRoot = getConfigStringHandleEmpty("capillary.storm.zkroot")

  private val SleepTimeMs = 1000
  private val MaxRetries = 3
  val retryPolicy = new ExponentialBackoffRetry(SleepTimeMs, MaxRetries)
  Logger.info(s"Kafka: starting new curator client to: $kafkaZookeepers")
  val zkKafkaClient = CuratorFrameworkFactory.newClient(kafkaZookeepers, retryPolicy)
  zkKafkaClient.start()
  // only build a new ZK client for storm if it connects to a different ZK cluster
  val zkStormClient = if (kafkaZookeepers != stormZookeepers) {
    Logger.info(s"Storm: starting new curator client to: $stormZookeepers")
    val newClient = CuratorFrameworkFactory.newClient(stormZookeepers, retryPolicy)
    newClient.start()
    newClient
  } else {
    Logger.info(s"Storm: using existing curator client to: $stormZookeepers")
    zkKafkaClient
  }

  def makePath(parts: Seq[Option[String]]): String = {
    parts.foldLeft("")({ (path, maybeP) => maybeP.map({ p => path + "/" + p }).getOrElse(path) }).replace("//", "/")
  }

  /**
    * this function is a bit vestigial. it's from when we supported trident, which we no longer used, so we removed the special
    * case code to simplify capillary. this method may be useful again someday, so we're keeping it.
    */
  def applyBase(path: Seq[Option[String]]): Seq[Option[String]] = {
    path
  }

  def getTopologies: Seq[Topology] = {
    zkStormClient.getChildren.forPath(makePath(Seq(stormZkRoot))).asScala.flatMap { r =>
      getSpoutTopology(r)
    }.sortWith(topoCompFn)
  }

  def getZkData(zkClient: CuratorFramework, path: String): Option[Array[Byte]] = {
    val maybeData = Option(zkClient.getData.forPath(path))

    // log a message if we get a null result for a path read
    if (maybeData.isEmpty) {
      Logger.error(s"Zookeeper Server ${zkClient.getZookeeperClient.getCurrentConnectionString} Path $path returned (null)!")
    }
    maybeData
  }

  def getSpoutTopology(root: String): Option[Topology] = {
    // Fetch the spout root
    zkStormClient.getChildren.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root))))).asScala.flatMap({ spout =>
      zkStormClient.getChildren.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(spout)))).asScala.flatMap({ part =>
        // Fetch the partitions so we can pick the first one
        val path = makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(spout)) ++ Seq(Some(part)))
        // Use the first partition's data to build up info about the topology
        // Also, don't trust zk to have valid JSON, it may be null or something...
        for {
          zkData <- getZkData(zkStormClient, path)
          state <- tryParse(zkData)
        } yield {
          val topic = (state \ "topic").as[String]
          val name = (state \ "topology" \ "name").as[String]
          Topology(name = name, topic = topic, spoutRoot = root)
        }
      })
    }).headOption
  }

  def getSpoutState(root: String, topic: String): Map[Int, Long] = {
    // There is basically nothing for error checking in here.
    val s = zkStormClient.getChildren.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root)))))
    // This gets us to the spout root
    s.asScala.flatMap({ pts =>
      // Fetch the partition information
      val parts = zkStormClient.getChildren.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(pts))))
      // For each partition, fetch the offsets, and collapse down the valid results into the return value
      parts.asScala.flatMap { p =>
        val jsonState = zkStormClient.getData.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(pts)) ++ Seq(Some(p))))
        tryParse(jsonState).map { state =>
          val offset = (state \ "offset").as[Long]
          val partition = (state \ "partition").as[Long]
          partition.toInt -> offset
        }
      }
    }).toMap
  }

  /**
    * Instead of parsing in kafka-main zookeeper:
    *  --> brokers/topics/TOPIC/partitions/PARTITION/state.LEADER
    * then
    *  --> brokers/ids/LEADER
    * to read host and port
    *
    * Use data from topology definition in Storm Zookeeper:
    * "topology":{
    *   "id":"0e318d74-ad8c-4c90-9fdd-625ce4622ac1",
    *   "name":"write_event8"
    *   },
    *   "offset":11164197691,
    *   "partition":9,
    *   "broker":{
    *     "host":"172.31.152.92",
    *     "port":9092},
    *   "topic":"migration"
    * }
    *
    * keen_storm/ -- level 0
    *   forward_event.backup_to_main-migration/ -- level 1
    *     forward_event.backup_to_main-migration/ -- level 2
    *       partition_0
    *       partition_1
    *       ...
    *       partition_15
    */
  def getKafkaState(root: String, topic: String): Map[Int, Long] = {
    val s = zkStormClient.getChildren.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root)))))  // level 0
    // This gets us to the spout root
    s.asScala.flatMap{ pts =>
      // Fetch the partition information
      val parts = zkStormClient.getChildren.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(pts))))  // level 1
      // For each partition, fetch the host and port
      parts.asScala.flatMap { p =>
        val jsonState = zkStormClient.getData.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(pts)) ++ Seq(Some(p)))) // level 2
        tryParse(jsonState).map { state =>
          val partition = (state \ "partition").as[Int]
          val host = (state \ "broker" \ "host").as[String]
          val port = (state \ "broker" \ "port").as[Int]
          Logger.info(s"Partition: '$partition', host: '$host', port: '$port'")

          val socketTimeout = 1000000
          val bufferSize = 64 * 1024
          // Talk to the lead broker and get offset data!
          val ks = new SimpleConsumer(host, port, socketTimeout, bufferSize, "capillary")
          val topicAndPartition = TopicAndPartition(topic, partition)
          val requestInfo = Map[TopicAndPartition, PartitionOffsetRequestInfo](
            topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)
          )
          val request = new OffsetRequest(
            requestInfo = requestInfo, versionId = OffsetRequest.CurrentVersion, clientId = "capillary")
          val response = ks.getOffsetsBefore(request)

          // in case of error, lets log some info and cease processing this partition rather than failing in the following map lookup
          if (response.hasError) {
            Logger.error("failed retrieving offsets for topic $topic and partition $partition, response was $response")
            None
          } else {
            val offset = response.partitionErrorAndOffsets(topicAndPartition).offsets.head
            ks.close
            Some(partition -> offset)
          }
        }.getOrElse(None)
      }
    }.toMap
  }

  def getKafkaState2(topic: String): Map[Int, Long] = {
    // Fetch info for each partition, given the topic
    val kParts = zkKafkaClient.getChildren.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/topics"), Some(topic), Some("partitions"))))
    // For each partition fetch the JSON state data to find the leader for each partition
    kParts.asScala.flatMap { kp =>
      val jsonState = zkKafkaClient.getData.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/topics"), Some(topic), Some("partitions"),
        Some(kp), Some("state"))))
      tryParse(jsonState).flatMap { state =>
        val leader = (state \ "leader").as[Long]

        // Knowing the leader's ID, fetch info about that host so we can contact it.
        val idJson = zkKafkaClient.getData.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/ids"), Some(leader.toString))))
        tryParse(idJson).map { leaderState =>
          val host = (leaderState \ "host").as[String]
          val port = (leaderState \ "port").as[Int]
          Logger.info(s"Partition: '$kp', host: '$host', port: '$port'")

          val socketTimeout = 5000
          val bufferSize = 64 * 1024
          // Talk to the lead broker and get offset data!
          val ks = new SimpleConsumer(host, port, socketTimeout, bufferSize, "capillary")
          val topicAndPartition = TopicAndPartition(topic, kp.toInt)
          val requestInfo = Map[TopicAndPartition, PartitionOffsetRequestInfo](
            topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)
          )
          val request = new OffsetRequest(
            requestInfo = requestInfo, versionId = OffsetRequest.CurrentVersion, clientId = "capillary")
          val response = ks.getOffsetsBefore(request)

          // in case of error, lets log some info and cease processing this partition rather than failing in the following map lookup
          if (response.hasError) {
            Logger.error("failed retrieving offsets for topic $topic and partition $kp, response was $response")
            None
          } else {
            val offset = response.partitionErrorAndOffsets(topicAndPartition).offsets.head
            ks.close
            Some(kp.toInt -> offset)
          }
        }
      }.getOrElse(None)
    }.toMap
  }

  def getTopologyDeltas(topoRoot: String, topic: String): (Totals, List[Delta]) = {
    val stormState = ZkKafka.getSpoutState(topoRoot, topic)

    Logger.info(s"KafkaState:")
    val zkState = ZkKafka.getKafkaState(topoRoot, topic)
    Logger.info(s"KafkaState: '$zkState'")

    Logger.info(s"KafkaState2:")
    val zkKafkaState = getKafkaState2(topic)
    Logger.info(s"KafkaState2: '$zkKafkaState'")

    var total = 0L
    var kafkaTotal = 0L
    var spoutTotal = 0L
    val deltas = zkState.map({ partAndOffset =>
      val partition = partAndOffset._1
      val koffset = partAndOffset._2
      // Try and pair up the partition information we got from ZK with the storm information
      // we got from the Storm state.
      stormState.get(partition).map({ soffset =>
        val amount = koffset - soffset
        total = amount + total
        kafkaTotal = koffset + kafkaTotal
        spoutTotal = soffset + spoutTotal
        Delta(partition = partition, amount = Some(amount), current = koffset, storm = Some(soffset))
      }).getOrElse({
        Logger.error(s"Storm State unavailable for partition $partition")
        Delta(partition = partition, amount = None, current = koffset, storm = None)
      })
    }).toList.sortBy(_.partition)

    (Totals(total, kafkaTotal, spoutTotal), deltas)
  }

  // Attempt to parse a byte array as json, and catch and log any error rather than
  // bubbling it up.
  def tryParse(json: Array[Byte]): Option[JsValue] = try {
    Some(Json.parse(json))
  } catch {
    case e: Exception =>
      val max_num_chars_for_error: Int = 100
      Logger.error(s"failed parsing json document:%n${json.take(max_num_chars_for_error)}".format(), e)
      None
  }
}
