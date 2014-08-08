package models

import com.twitter.zookeeper.ZooKeeperClient
import kafka.api.{OffsetFetchRequest,OffsetRequest,PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import play.api.libs.json._
import play.api.Play
import play.api.Play.current

object ZkKafka {

  case class Delta(partition: Int, amount: Option[Long], current: Long, storm: Option[Long])

  val zookeepers = Play.configuration.getString("capillary.zookeepers").getOrElse("localhost:2181")
  val kafkaZkRoot = Play.configuration.getString("capillary.kafka.zkroot").getOrElse("")
  lazy val zk = new ZooKeeperClient(zookeepers)

  def getSpoutState(root: String, topic: String): Map[Int, Long] = {

    val s = zk.getChildren(s"/$root")
    // println(s.length)
    // println(s(0))

    val parts = zk.getChildren(s"/$root/" + s(0))

    // This is here because staging has old and new partition znodes :(
    val validParts = parts.filter { p => p.startsWith("partition") }
    // println(validParts)

    return validParts.map { vp =>
      val jsonState = new String(zk.get(s"/$root/" + s(0) + s"/$vp"))
      val state = Json.parse(jsonState)
      val offset = (state \ "offset").as[Long]
      val partition = (state \ "partition").as[Long]
      val ttopic = (state \ "topic").as[String]
      // println(s"$ttopic $partition $offset")
      (partition.toInt, offset)
    } toMap
  }

  def getKafkaState(topic: String): Map[Int, Long] = {

    val kParts = zk.getChildren(s"/$kafkaZkRoot/brokers/topics/$topic/partitions")
    kParts.map { kp =>
      val jsonState = new String(zk.get(s"/$kafkaZkRoot/brokers/topics/$topic/partitions/$kp/state"))
      val state = Json.parse(jsonState)
      val leader = (state \ "leader").as[Long]
      // println(s"p: $kp, l: $leader")

      val idJson = new String(zk.get(s"/$kafkaZkRoot/brokers/ids/$leader"))
      val leaderState = Json.parse(idJson)
      val host = (leaderState \ "host").as[String]
      val port = (leaderState \ "port").as[Int]
      // println(s"h: $host, p: $port")

      val ks = new SimpleConsumer(host, port, 1000000, 64*1024, "test-client")
      val topicAndPartition = TopicAndPartition(topic, kp.toInt)
      val requestInfo = Map[TopicAndPartition, PartitionOffsetRequestInfo](
          topicAndPartition -> new PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)
      )
      val request = new OffsetRequest(
        requestInfo = requestInfo, versionId = OffsetRequest.CurrentVersion, clientId = "test-client")
      val response = ks.getOffsetsBefore(request);
      if(response.hasError) {
        println("ERROR!")
      }
      (kp.toInt, response.partitionErrorAndOffsets.get(topicAndPartition).get.offsets(0))
      // println(response)
    } toMap
  }
}