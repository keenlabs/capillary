package models

import java.io.{ByteArrayInputStream, InputStreamReader}
import java.util.zip.{GZIPInputStream, ZipException}

import com.google.common.io.CharStreams
import kafka.api.{FetchRequestBuilder, PartitionMetadata, TopicMetadataRequest}
import kafka.consumer.SimpleConsumer
import play.api.Play.current
import play.api.{Logger, Play}

/**
  * Contains code to read messages from kafka.
  */
object KafkaMessageReader {

  private val Kafkas = Play.configuration.getString("capillary.kafkas").getOrElse("127.0.0.1:9092")
  private val KafkaHosts = Kafkas.split(",")

  private val Timeout = 100000
  private val BufferSize = 10000000
  private val ClientId = "Capillary"

  def getMessageContent(topic: String, partition: Int, offset: Long): String = {
    findLeader(KafkaHosts, topic, partition) match {
      case Some(pm) => readMessage(pm, topic, offset)
      case None => s"Could not find any leader for partition: $partition"
    }
  }

  private def findLeader(kafkaHosts: Array[String], topic: String, partition: Int): Option[PartitionMetadata] = {
    var simpleConsumer: Option[SimpleConsumer] = None
    try {
      for (kafkaHost <- kafkaHosts) {
        val hostAndPort = kafkaHost.split(":")
        val host = hostAndPort(0)
        val port = hostAndPort(1).toInt
        simpleConsumer = Some(new SimpleConsumer(host, port, Timeout, BufferSize, ClientId))
        val topicMetadataRequest = new TopicMetadataRequest(Seq(topic), 0)
        val topicMetadataResponse = simpleConsumer.get.send(topicMetadataRequest)
        for (topicMetadata <- topicMetadataResponse.topicsMetadata) {
          for (partitionMetadata <- topicMetadata.partitionsMetadata) {
            if (partitionMetadata.partitionId == partition) {
              Logger.debug(s"FoundPartitionMetadata, p=$partitionMetadata")
              // scalastyle:off return
              return Some(partitionMetadata)
              // scalastyle:on return
            }
          }
        }
      }
    } finally {
      if (simpleConsumer.isDefined) {
        simpleConsumer.get.close()
      }
    }

    Logger.error("No partition found")
    None
  }

  private def readMessage(partitionMetadata: PartitionMetadata, topic: String, offset: Long): String = {
    val leadBroker = partitionMetadata.leader.get
    val simpleConsumer = new SimpleConsumer(leadBroker.host, leadBroker.port, Timeout, BufferSize, ClientId)
    try {
      val fetchRequest = new FetchRequestBuilder()
        .clientId(ClientId)
        .addFetch(topic, partitionMetadata.partitionId, offset, BufferSize)
        .build()
      val fetchResponse = simpleConsumer.fetch(fetchRequest)
      if (fetchResponse.hasError) {
        val errorMessage = s"Error while fetching from topic: $topic and offset: $offset. " +
          s"Error code: ${fetchResponse.errorCode(topic, partitionMetadata.partitionId)}"
        Logger.error(errorMessage)
        // scalastyle:off return
        return errorMessage
        // scalastyle:on return
      }

      val messagesAndOffsets = fetchResponse.messageSet(topic, partitionMetadata.partitionId)
      messagesAndOffsets.headOption.map { messageAndOffset =>
        val payload = messageAndOffset.message.payload
        val bytes = new Array[Byte](payload.limit())
        payload.get(bytes)
        val str = readBytesAsGzippedString(bytes).getOrElse(readBytesAsString(bytes))
        // scalastyle:off return
        return str
        // scalastyle:on return
      }
    } finally {
      simpleConsumer.close()
    }

    ""
  }

  private def readBytesAsString(bytes: Array[Byte]) = {
    new String(bytes, "UTF-8")
  }

  private def readBytesAsGzippedString(bytes: Array[Byte]): Option[String] = {
    try {
      Some(CharStreams.toString(new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(bytes)))))
    } catch {
      case e: ZipException => None
    }
  }

}
