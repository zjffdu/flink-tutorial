package org.apache.zjffdu.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkWordCount {


  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    val data = senv.fromElements("hello world", "hello spark", "hello hadoop")
    data.flatMap(line => line.split("\\s"))
      .map(w => (w, 1))
      .keyBy(0)
      .sum(1)
      .print()

    senv.execute()
  }
}
