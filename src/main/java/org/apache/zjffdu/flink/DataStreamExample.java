package org.apache.zjffdu.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class DataStreamExample {


  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

    List<Tuple3<Long, Integer, List<String>>> input = new ArrayList<>();

    for (int i=0;i<1000;++i) {
      input.add(new Tuple3(System.currentTimeMillis() + i * 1000, 1, Lists.newArrayList("hello")));
      input.add(new Tuple3(System.currentTimeMillis() + i * 1000, 2,  Lists.newArrayList("hi")));
      input.add(new Tuple3(System.currentTimeMillis() + i * 1000, 2, Lists.newArrayList("world")));
      input.add(new Tuple3(System.currentTimeMillis() + i * 1000, 1, Lists.newArrayList("world")));
    }


    DataStream<Tuple3<Long, Integer, List<String>>> ds = senv.fromCollection(input);

    DataStream<Tuple3<Long, Integer, List<String>>> result = ds.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Integer, List<String>>>() {
      @Override
      public long extractAscendingTimestamp(Tuple3<Long, Integer, List<String>> integerListTuple2) {
        return integerListTuple2.f0;
      }
    }).keyBy(1)
            .timeWindow(Time.seconds(1))
            .reduce((a,b) -> {
      List<String> merged = new ArrayList<>();
      merged.addAll(a.f2);
      merged.addAll(b.f2);
      return Tuple3.of(a.f0, a.f1, b.f2);
    });

    result.print("************");

    senv.execute();
  }
}
