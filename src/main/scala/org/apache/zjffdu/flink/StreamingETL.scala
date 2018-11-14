package org.apache.zjffdu.flink

import java.sql.Timestamp
import java.util.Properties

import com.google.gson.Gson
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row

object StreamingETL {


  class TimestampAndWatermarkWithOffset[T <: Product](
                                                       offset: Long) extends AssignerWithPunctuatedWatermarks[T] {

    override def checkAndGetNextWatermark(
                                           lastElement: T,
                                           extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp - offset)
    }

    override def extractTimestamp(
                                   element: T,
                                   previousElementTimestamp: Long): Long = {
      element.productElement(2).asInstanceOf[Long]
    }
  }

  case class Record(status: String, direction: String, var event_ts: Long)
  case class NewRecord(status: String, direction: String, var event_ts: Timestamp)


  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setParallelism(1)
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    val sourceData = senv.addSource(new FlinkKafkaConsumer011[String]("generated.events", new SimpleStringSchema(), properties));
    val data: DataStream[NewRecord] = sourceData.map(line => {
      val gson = new Gson()
      val record = gson.fromJson(line, classOf[Record])
      NewRecord(record.status, record.direction, new Timestamp(record.event_ts))
    })

    val tEnv = TableEnvironment.getTableEnvironment(senv)
    val sourceSchema = new Schema()
      .field("status", Types.STRING)
      .field("direction", Types.STRING)
      .field("rowtime", Types.SQL_TIMESTAMP).rowtime(
      new Rowtime().timestampsFromField("event_ts").watermarksPeriodicAscending())


    val sinkSchema = new Schema()
      .field("status", Types.STRING)
      .field("direction", Types.STRING)
      .field("rowtime", Types.SQL_TIMESTAMP)


    val tableDesc = tEnv
      // declare the external system to connect to
      .connect(
      new Kafka()
        .version("0.11")
        .topic("processed5.events")
        .startFromEarliest()
        .property("zookeeper.connect", "localhost:2181")
        .property("bootstrap.servers", "localhost:9092"))
      .withFormat(new Json()
        .failOnMissingField(false)
        .deriveSchema()
      )
      .withSchema(
        new Schema()
          .field("status", Types.STRING)
          .field("direction", Types.STRING)
          .field("event_ts", Types.SQL_TIMESTAMP).rowtime(
          new Rowtime().timestampsFromField("event_ts").watermarksPeriodicAscending())
      )

      // specify the update-mode for streaming tables
      .inAppendMode()


    tableDesc.withSchema(sourceSchema)
      .registerTableSource("MyUserTable");

    tableDesc.withSchema(sinkSchema)
      .registerTableSink("MyUserTable");

    tEnv.fromDataStream(data).insertInto("MyUserTable")

    senv.submit()

    tEnv.sqlQuery("select * from MyUserTable")
      .toAppendStream[Row].print("**********")

    senv.execute()
  }

}
