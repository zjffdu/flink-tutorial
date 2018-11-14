package org.apache.zjffdu.flink

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object StreamTableJob {

  case class A(name: String, a: String)

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
      element.productElement(0).asInstanceOf[Long]
    }
  }

  def main(args: Array[String]): Unit = {

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(senv)
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    senv.setParallelism(1)

    val sessionWindowTestData = List(
      (1L, 2, "Hello"),       // (1, Hello)       - window
      (2L, 2, "Hello"),       // (1, Hello)       - window, deduped
      (8L, 2, "Hello"),       // (2, Hello)       - window, deduped during merge
      (10L, 3, "Hello"),      // (2, Hello)       - window, forwarded during merge
      (9L, 9, "Hello World"), // (1, Hello World) - window
      (4L, 1, "Hello"),       // (1, Hello)       - window, triggering merge
      (16L, 16, "Hello"))     // (3, Hello)       - window (not merged)

    val stream = senv
      .fromCollection(sessionWindowTestData)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset[(Long, Int, String)](10L))

    val table = stream.toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)
    tEnv.registerTable("MyTable", table)

    val sqlQuery = "SELECT c, " +
      "  COUNT(DISTINCT b)," +
      "  SESSION_END(rowtime, INTERVAL '0.005' SECOND) " +
      "FROM MyTable " +
      "GROUP BY SESSION(rowtime, INTERVAL '0.005' SECOND), c "

    val results = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    results.print()
//    results.addSink(new StreamITCase.StringSink[Row])
    senv.execute()

//    FileUtils.deleteDirectory(new File("/Users/jzhang/Temp/a"))
//    val senv = StreamExecutionEnvironment.getExecutionEnvironment
//
//
//    val stEnv = TableEnvironment.getTableEnvironment(senv)
//
//    val data = senv.fromCollection(1 to 20).map(e => {
//      Thread.sleep(3000); e
//    })
//    stEnv.registerDataStream("test", data)
//    stEnv.connect(new FileSystem().path("file:///Users/jzhang/Temp/a")).
//      withFormat(new Csv().fieldDelimiter("\n").
//        field("f0", "INT")).
//      withSchema(new Schema().field("f0", "INT")).
//      inAppendMode().
//      registerTableSourceAndSink("dest")
//
//    stEnv.sqlQuery("select * from test where f0 > 2").insertInto("dest")
//
////    senv.execute(true)
//
//    stEnv.sqlQuery("select count(1) from dest").toRetractStream[Row].print()
//    senv.execute()
  }
}
