package com.kpn.datalab

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.{DateTimeBucketer, RollingSink}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer.{FetcherType, OffsetStore}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

object Job {
  def main(args: Array[String]) {

    val batchSize = 1024 * 1024 * 500

    // We get the current environment. When executed from the IDE this will create a Flink mini-cluster,
    // otherwise it accesses the current cluster environment.
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConsumerProperties = new Properties()
    kafkaConsumerProperties.setProperty("zookeeper.connect", "localhost:2181")
    kafkaConsumerProperties.setProperty("group.id", "flink")
    kafkaConsumerProperties.setProperty("bootstrap.servers", "localhost:9092")

    val kafkaConsumer = new FlinkKafkaConsumer[String](
      "topic",
      KafkaStringSchema,
      kafkaConsumerProperties,
      OffsetStore.FLINK_ZOOKEEPER,
      FetcherType.LEGACY_LOW_LEVEL
    )

    val stream = env.addSource(kafkaConsumer)

    val sink = new RollingSink[String]("/tmp/dump") //"hdfs:///user/sanne"
      .setBucketer(new DateTimeBucketer("ss"))
      .setBatchSize(batchSize)
      .setPartPrefix("part")
      .setPendingPrefix("")
      .setPendingSuffix("");

    stream.addSink(sink)

    env.execute("Window Stream WordCount")
  }

  object KafkaStringSchema extends SerializationSchema[String, Array[Byte]] with DeserializationSchema[String] {

    import org.apache.flink.api.common.typeinfo.TypeInformation
    import org.apache.flink.api.java.typeutils.TypeExtractor

    override def serialize(t: String): Array[Byte] = t.getBytes("UTF-8")

    override def isEndOfStream(t: String): Boolean = false

    override def deserialize(bytes: Array[Byte]): String = new String(bytes, "UTF-8")

    override def getProducedType: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  }

}