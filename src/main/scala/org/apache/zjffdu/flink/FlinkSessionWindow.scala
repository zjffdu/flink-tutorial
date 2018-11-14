package org.apache.zjffdu.flink

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time


object FlinkSessionWindow {

  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    senv.setParallelism(1)

    val input = Seq(
      ("a", 1L, 1),
      ("b", 1L, 1),
      ("b", 3L, 1),
      ("b", 5L, 1),
      ("c", 6L, 1),
      // We expect to detect the session "a" earlier than this point (the old
      // functionality can only detect here when the next starts)
      ("a", 10L, 1),
      // We expect to detect session "b" and "c" at this point as well
      ("c", 11L, 1)
    )

    val data = senv.addSource(new SourceFunction[(String, Long, Int)]{
      override def run(ctx: SourceFunction.SourceContext[(String, Long, Int)]): Unit = {
        input.foreach(record => {
          ctx.collectWithTimestamp(record, record._2)
          ctx.emitWatermark(new Watermark(record._2 - 1))
        })

        ctx.emitWatermark(new Watermark(Long.MaxValue))
      }

      override def cancel(): Unit = {

      }
    })

    data.map(e=>(e._1, e._3))
      .keyBy(0)
      .window(EventTimeSessionWindows.withGap(Time.milliseconds(3)))
      .sum(1)
      .print()

    senv.execute()
  }
}
