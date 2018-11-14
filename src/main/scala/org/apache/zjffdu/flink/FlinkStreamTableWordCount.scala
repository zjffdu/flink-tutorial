//package org.apache.zjffdu.flink
//
//import org.apache.flink.api.scala.ExecutionEnvironment
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.table.api.TableEnvironment
//import org.apache.flink.types.Row
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.api.datastream.DataStreamUtils
//import org.apache.flink.table.api.scala._
//
//
//object FlinkStreamTableWordCount {
//
//  def main(args: Array[String]): Unit = {
//    val senv = StreamExecutionEnvironment.getExecutionEnvironment
//    senv.setParallelism(1)
//
//    val tsEnv = StreamTableEnvironment.create(senv)
//
//    val data = senv.fromElements("hello world", "hello flink", "hello hadoop")
//
//    val table = data.flatMap(line => line.split("\\s"))
//      .map(w => (w, 1))
//      .toTable(tsEnv, 'word, 'number)
//
//    tsEnv.registerTable("wc", table)
//
//    val resultStream = tsEnv.sqlQuery("select word, number from wc")
//      .toAppendStream[Row]
//
//
////    senv.execute()
//  }
//}
