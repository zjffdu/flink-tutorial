//package org.apache.zjffdu.flink
//
//import java.sql.Timestamp
//import java.util.Properties
//
//import com.fasterxml.jackson.databind.node.ObjectNode
//import com.google.gson.Gson
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.scala._
//import org.apache.flink.table.api.scala._
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.datastream.DataStreamUtils
//import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.streaming.api.watermark.Watermark
//import org.apache.flink.streaming.connectors.fs.{DateTimeBucketer, RollingSink}
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
//import org.apache.flink.table.api.{TableEnvironment, Types}
//import org.apache.flink.table.descriptors._
//import org.apache.flink.types.Row
//
//object StreamingETLDemo {
//
//
//  class TimestampAndWatermarkWithOffset[T <: Product](
//                                                       offset: Long) extends AssignerWithPunctuatedWatermarks[T] {
//
//    override def checkAndGetNextWatermark(
//                                           lastElement: T,
//                                           extractedTimestamp: Long): Watermark = {
//      new Watermark(extractedTimestamp - offset)
//    }
//
//    override def extractTimestamp(
//                                   element: T,
//                                   previousElementTimestamp: Long): Long = {
//      element.productElement(2).asInstanceOf[Long]
//    }
//  }
//
//  case class Record(status: String, direction: String, var event_ts: Long)
//  case class NewRecord(status: String, direction: String, var event_ts: Timestamp)
//
//
//  def main(args: Array[String]): Unit = {
//
//    val senv = StreamExecutionEnvironment.getExecutionEnvironment
//    senv.setParallelism(1)
//    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", "localhost:9092")
//    val sourceData = senv.addSource(new FlinkKafkaConsumer011[String]("generated.events", new SimpleStringSchema(), properties));
//    val data: DataStream[String] = sourceData.map(line => {
//      val gson = new Gson()
//      val record = gson.fromJson(line, classOf[Record])
//      record.status + "\t" + record.direction + "\t" + record.event_ts
//    })
//
//    val rollingSink = new RollingSink[String]("/tmp/kafka-output")
//    rollingSink.setBucketer(new DateTimeBucketer("yyyy-MM-dd-HH-mm")) // do a bucket for each minute
//
//    data.addSink(rollingSink).name("Rolling FileSystem Sink")
//    senv.execute()
//  }
//
//}
