package org.apache.zjffdu.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object FlinkWindowWordCount {

  def main(args: Array[String]): Unit = {

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setParallelism(1)
    val data = senv.fromElements("hello world", "hello flink", "hello hadoop")

    data.flatMap(line => line.split("\\s"))
      .map(w => (w, 1))
      .keyBy(0)
      .countWindow(2, 1)
      .sum(1)
      .print("******************")

    senv.execute()

  }
}
