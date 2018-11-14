package org.apache.zjffdu.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Random;
import java.util.UUID;

public class LargeScaleWordCount {

  public static void main(String[] args) throws Exception {
    final ParameterTool params = ParameterTool.fromArgs(args);

    Configuration conf = new Configuration();
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

    StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    senv.enableCheckpointing(5000);

    DataStream<Tuple2<Long, String>> ds = senv.addSource(new WordGenerator(params.getInt("factor", 1)));
    DataStream<Tuple3<Long, String, Long>> result = ds.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<Long, String>>(Time.seconds(5)) {

      @Override
      public long extractTimestamp(Tuple2<Long, String> input) {
        return input.f0;
      }
    }).flatMap(new FlatMapFunction<Tuple2<Long, String>, Tuple3<Long, String, Long>>() {
      @Override
      public void flatMap(Tuple2<Long, String> longStringTuple2, Collector<Tuple3<Long, String, Long>> collector) throws Exception {
        collector.collect(Tuple3.of(longStringTuple2.f0, longStringTuple2.f1, 1L));
      }
    }).keyBy(1)
    .timeWindow(Time.seconds(5))
    .reduce(new ReduceFunction<Tuple3<Long, String, Long>>() {
      @Override
      public Tuple3<Long, String, Long> reduce(Tuple3<Long, String, Long> reduced, Tuple3<Long, String, Long> t1) throws Exception {
        return Tuple3.of(reduced.f0, reduced.f1, reduced.f2 + t1.f2);
      }
    });

    result.print();

    senv.execute("WordCount");


  }

  public static class WordGenerator implements ParallelSourceFunction<Tuple2<Long, String>> {

    private transient boolean stopped = false;
    private transient UUID uuid = UUID.randomUUID();
    private int factor = 2;

    public WordGenerator(int factor) {
        this.factor = factor;
    }

    @Override
    public void run(SourceContext<Tuple2<Long, String>> context) throws Exception {
      while(!stopped) {
        context.collect(Tuple2.of(System.currentTimeMillis(), UUID.randomUUID().toString().substring(0, factor)));
      }
    }

    @Override
    public void cancel() {

    }
  }
}
