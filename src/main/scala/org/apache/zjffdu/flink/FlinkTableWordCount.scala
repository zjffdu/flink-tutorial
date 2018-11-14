package org.apache.zjffdu.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object FlinkTableWordCount {

  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val env = ExecutionEnvironment.getExecutionEnvironment

    senv.setParallelism(1)

    val tsEnv = TableEnvironment.getTableEnvironment(senv)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val data = env.fromElements("hello world", "hello flink", "hello hadoop")

    val table = data.flatMap(line=>line.split("\\s"))
      .map(w => (w, 1))
      .toTable(tEnv, 'word, 'count)

    tEnv.registerTable("wc", table)

    tEnv.sqlQuery("select word, count(1) from wc group by word")
      .toDataSet[Row].print()

//    senv.execute()
  }
}
