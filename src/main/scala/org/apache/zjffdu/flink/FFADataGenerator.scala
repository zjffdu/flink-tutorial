package org.apache.zjffdu.flink

import java.util.Collections

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.collection.JavaConversions._

object FFADataGenerator {

  def main(args: Array[String]): Unit = {

    val senv = StreamExecutionEnvironment.getExecutionEnvironment()
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    senv.enableCheckpointing(1000)
    val MAX_COUNT = if (args.length == 0) 1000 else args(0).toInt
    val data = senv.addSource(new SourceFunction[org.apache.flink.api.java.tuple.Tuple2[Long, String]] {

      val pages = Seq("home", "search", "search", "product", "product", "product")
      var count: Long = 0
      // startTime is 2019/1/1
      var startTime: Long = new java.util.Date(2019 - 1900,0,1).getTime
      var sleepInterval = MAX_COUNT

      override def run(ctx: SourceFunction.SourceContext[org.apache.flink.api.java.tuple.Tuple2[Long, String]]): Unit = {
        val lock = ctx.getCheckpointLock

        while (count < MAX_COUNT) {
          lock.synchronized({
            ctx.collect(org.apache.flink.api.java.tuple.Tuple2.of(startTime + count * sleepInterval, pages(count.toInt % pages.size)))
            count += 1
          })
        }
      }

      override def cancel(): Unit = {

      }

    })

    val dest = if (args.length < 2) "/tmp/csv.txt" else args(1)
    data.writeAsCsv(dest, WriteMode.OVERWRITE)
    senv.execute("generating data")
  }
}
