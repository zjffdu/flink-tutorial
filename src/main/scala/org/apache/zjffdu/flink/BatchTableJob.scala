//package org.apache.zjffdu.flink
//
//
//import org.apache.flink.api.scala._
//import org.apache.flink.core.fs.FileSystem
//import org.apache.flink.table.api.TableEnvironment
//import org.apache.flink.types.Row
//import org.apache.flink.table.api.scala._
//
//
//object BatchTableJob {
//
//  def main(args: Array[String]): Unit = {
//
//    val benv = ExecutionEnvironment.getExecutionEnvironment
//    val btEnv = BatchTableEnvironment.create(benv)
//
//    val data = benv.fromElements("hello world", "hello flink", "hello hadoop")
//
//    data.writeAsText("/Users/jzhang/a.txt", FileSystem.WriteMode.OVERWRITE);
//
//    val table = data.flatMap(line=>line.split("\\s")).
//      map(w => (w, 1)).
//      toTable(btEnv, 'word, 'number)
//
//    btEnv.registerTable("wc", table)
//
//    btEnv.sqlQuery("select word, count(1) from wc group by word").
//      toDataSet[Row].print()
//
//  }
//}
