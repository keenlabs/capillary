package models

import kafka.api.{OffsetFetchRequest,OffsetRequest,PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.retry.ExponentialBackoffRetry
import play.api.libs.json._
import play.api.Logger
import play.api.Play
import play.api.Play.current
import scala.collection.JavaConverters._

object ZkKafka {
  case class Totals(total: Long, kafkaTotal: Long, spoutTotal: Long)
  case class Delta(partition: Int, amount: Option[Long], current: Long, storm: Option[Long])
  case class Topology(name: String, spoutRoot: String, topic: String)

  def topoCompFn(t1: Topology, t2: Topology) = {
    (t1.name compareToIgnoreCase t2.name) < 0
  }

  val zookeepers = Play.configuration.getString("capillary.zookeepers").getOrElse("localhost:2181")
  val kafkaZkRoot = Play.configuration.getString("capillary.kafka.zkroot")
  val stormZkRoot = Play.configuration.getString("capillary.storm.zkroot")
  val isTrident = Play.configuration.getString("capillary.use.trident").getOrElse(false)

  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val zkClient = CuratorFrameworkFactory.newClient(zookeepers, retryPolicy);
  zkClient.start();

  def makePath(parts: Seq[Option[String]]): String = {
    parts.foldLeft("")({ (path, maybeP) => maybeP.map({ p => path + "/" + p }).getOrElse(path) }).replace("//","/")
  }

  def applyBase(path: Seq[Option[String]]): Seq[Option[String]] = {
    if(isTrident.equals("true")) path ++ Seq(Some("user")) else path
  }

  def getTopologies: Seq[Topology] = {
    zkClient.getChildren.forPath(makePath(Seq(stormZkRoot))).asScala.map({ r =>
      getSpoutTopology(r)
    }).flatten.sortWith(topoCompFn)
  }

  def getZkData(path: String): Option[Array[Byte]] = {
    val maybeData = Option(zkClient.getData.forPath(path))
    if ( maybeData.isEmpty ) {
      Logger.error("Zookeeper Path " + path + " returned (null)!")
    }
    maybeData
  }

  def getSpoutTopology(root: String): Option[Topology] = {
    // Fetch the spout root
    zkClient.getChildren.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root))))).asScala.flatMap({ spout =>
      zkClient.getChildren.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(spout)))).asScala.flatMap({ part =>
        // Fetch the partitions so we can pick the first one
        val path = makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(spout)) ++ Seq(Some(part)))
        // Use the first partition's data to build up info about the topology
        // Also, don't trust zk to have valid JSON, it may be null or something...
        for {
          zkData <- getZkData(path)
          state <- tryParse(zkData)
        } yield {
          val topic = (state \ "topic").as[String]
          val name  = (state \ "topology" \ "name").as[String]
          Topology(name = name, topic = topic, spoutRoot = root)
        }
      })
    }).headOption
  }

  def getSpoutState(root: String, topic: String): Map[Int, Long] = {
    // There is basically nothing for error checking in here.
    val s = zkClient.getChildren.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root)))))
    // This gets us to the spout root
    s.asScala.flatMap({ pts =>
      // Fetch the partition information
      val parts = zkClient.getChildren.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(pts))))
      // For each partition, fetch the offsets.
      parts.asScala.map({ p =>
        val jsonState = zkClient.getData.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(pts)) ++ Seq(Some(p))))
        tryParse(jsonState).map { state =>
          val offset = (state \ "offset").as[Long]
          val partition = (state \ "partition").as[Long]
          Some(partition.toInt -> offset)
        }.getOrElse(None)
      }).filter(_.isDefined).map(_.get)
    }).toMap
  }

  def getKafkaState(topic: String): Map[Int, Long] = {
    // Fetch info for each partition, given the topic
    val kParts = zkClient.getChildren.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/topics"), Some(topic), Some("partitions"))))
    // For each partition fetch the JSON state data to find the leader for each partition
    kParts.asScala.map({ kp =>
      val jsonState = zkClient.getData.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/topics"), Some(topic), Some("partitions"), Some(kp), Some("state"))))
      tryParse(jsonState).flatMap { state =>
        val leader = (state \ "leader").as[Long]

        // Knowing the leader's ID, fetch info about that host so we can contact it.
        val idJson = zkClient.getData.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/ids"), Some(leader.toString))))
        tryParse(idJson).map { leaderState =>
          val host = (leaderState \ "host").as[String]
          val port = (leaderState \ "port").as[Int]

          // Talk to the lead broker and get offset data!
          val ks = new SimpleConsumer(host, port, 1000000, 64*1024, "capillary")
          val topicAndPartition = TopicAndPartition(topic, kp.toInt)
          val requestInfo = Map[TopicAndPartition, PartitionOffsetRequestInfo](
            topicAndPartition -> new PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)
          )
          val request = new OffsetRequest(
            requestInfo = requestInfo, versionId = OffsetRequest.CurrentVersion, clientId = "capillary")
          val response = ks.getOffsetsBefore(request);

          // in case of error, lets log some info and cease processing this partition rather than failing in the following map lookup
          if (response.hasError) {
            Logger.error("failed retrieving offsets for topic $topic and partition $kp, response was $response");
            None
          } else {
            val offset = response.partitionErrorAndOffsets(topicAndPartition).offsets(0)
            ks.close
            Some(kp.toInt -> offset)
          }
        }
      }.getOrElse(None)
    }).filter(_.isDefined).map(_.get).toMap
  }

  def getTopologyDeltas(topoRoot: String, topic: String): Tuple2[Totals, List[Delta]] = {
    val stormState = ZkKafka.getSpoutState(topoRoot, topic)

    val zkState = ZkKafka.getKafkaState(topic)
    var total = 0L;
    var kafkaTotal = 0L;
    var spoutTotal = 0L;
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
        Logger.error(s"Storm State unavailable for partition ${partition}")
        Delta(partition = partition, amount = None, current = koffset, storm = None)
      })
    }).toList.sortBy(_.partition)

    (new Totals(total, kafkaTotal, spoutTotal), deltas)
  }

  // Attempt to parse a byte array as json, and catch and log tany error rather than
  // bubbling it up.
  def tryParse (json: Array[Byte]): Option[JsValue] = try {
    Some(Json.parse(json))
  } catch {
    case e: Exception =>
      Logger.error(s"failed parsing json document:\n${json.take(100)}", e)
      None
  }
}
