package org.apache.zjffdu.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object KafkaSinkExample {

  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val data = senv.fromElements(1,2,3,4,5)
      .map(e=>e+"")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    val sink = new FlinkKafkaProducer011[String]("localhost:9092", "test", new SimpleStringSchema())
    data.addSink(sink)
    senv.execute()
  }
}
