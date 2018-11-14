/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zjffdu.flink

import java.util.Collections

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConversions._


//class CounterSource
//  extends RichParallelSourceFunction[Long]
//    with ListCheckpointed[Long] {
//
//  @volatile
//  private var isRunning = true
//
//  private var offset = 0L
//
//  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
//    val lock = ctx.getCheckpointLock
//
//    while (isRunning) {
//      // output and state update are atomic
//      lock.synchronized({
//        ctx.collect(offset)
//
//        offset += 1
//      })
//    }
//  }
//
//  override def cancel(): Unit = isRunning = false
//
//  override def restoreState(state: util.List[Long]): Unit =
//    for (s <- state) {
//      offset = s
//    }
//
//  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Long] =
//    Collections.singletonList(offset)
//
//}

/**
  * Skeleton for a Flink Streaming Job.
  *
  * For a tutorial how to write a Flink streaming application, check the
  * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
  *
  * To package your application into a JAR file for execution, run
  * 'mvn clean package' on the command line.
  *
  * If you change the name of the main class (with the public static void main(String[] args))
  * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
  */
object StreamingJob {

  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

  def main(args: Array[String]) {


    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    senv.enableCheckpointing(1000)

//    System.getProperty()
    val data = senv.addSource(new SourceFunction[(Long, String)] with ListCheckpointed[java.lang.Long] {

      val pages = Seq("home", "search", "search", "product", "product", "product")
      var count: Long = 0

      override def run(ctx: SourceFunction.SourceContext[(Long, String)]): Unit = {
        val lock = ctx.getCheckpointLock

        while (count < 100000) {
          lock.synchronized({
            ctx.collect((count, pages(count.toInt % pages.size)))
            count += 1
            Thread.sleep(1000)
          })
        }
      }

      override def cancel(): Unit = {

      }

      override def snapshotState(checkpointId: Long, timestamp: Long): java.util.List[java.lang.Long] = {
        Collections.singletonList(count)
      }


      override def restoreState(state: java.util.List[java.lang.Long]): Unit = {
        state.foreach(s => count = s)
      }

    }).assignAscendingTimestamps(_._1)

//    val stEnv = StreamTableEnvironment.create(senv)

//    stEnv.registerOrReplaceDataStream("log", data, 'time, 'url, 't.rowtime)

    data.print(">>>>>>>>>>>>>>>>>>>")

    senv.execute()


  }
}
