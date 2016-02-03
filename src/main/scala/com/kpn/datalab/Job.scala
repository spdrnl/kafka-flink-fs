import java.util.concurrent.TimeUnit
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer.{FetcherType, OffsetStore}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

object Job {
  def main(args: Array[String]) {

    // We get the current environment. When executed from the IDE this will create a Flink mini-cluster,
    // otherwise it accesses the current cluster environment.
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConsumerProperties = Map(
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "flink",
      "bootstrap.servers" -> "localhost:9092"
    )

    val kafkaConsumer = new FlinkKafkaConsumer[String](
      "topic",
      KafkaStringSchema,
      kafkaConsumerProperties,
      OffsetStore.FLINK_ZOOKEEPER,
      FetcherType.LEGACY_LOW_LEVEL
    )

    val stream = env.addSource(kafkaConsumer)

    val counts = stream.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }
      .map {
      (_, 1)
    }
      .keyBy(0)
      .timeWindow(Time.of(5, TimeUnit.SECONDS))
      .sum(1)

    counts.print

    env.execute("Window Stream WordCount")
  }

  implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
    (new java.util.Properties /: map) { case (props, (k, v)) => props.put(k, v); props }
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