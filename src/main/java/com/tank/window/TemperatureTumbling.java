package com.tank.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nonnull;

/**
 * @author tank198435163.com
 */
public class TemperatureTumbling {

  public void processDataFromSocket() throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final int parallelism = Math.max(Runtime.getRuntime().availableProcessors(), 1);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(parallelism);

    processData(env, ip, port);

    env.executeAsync("sensor data processed from socket")
            .getJobStatus()
            .whenComplete((d, err) -> System.out.println(String.format("job name is:[%s]", d.name())));
  }

  private void processData(@Nonnull final StreamExecutionEnvironment env,
                           @Nonnull String ip,
                           @Nonnull Integer port) {

    env.socketTextStream(ip, port)
            .map((MapFunction<String, Temperature>) input -> {
              final String[] arr = input.split(",");
              Temperature sensor = new Temperature();
              String id = arr[0];
              Long timeStamp = Long.parseLong(arr[1]);
              Double value = Double.parseDouble(arr[2]);
              sensor.id = id;
              sensor.timeStamp = timeStamp;
              sensor.value = value;
              return sensor;
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Temperature>(Time.seconds(1)) {
              @Override
              public long extractTimestamp(Temperature element) {
                return element.timeStamp * 1000;
              }
            })
            .map(new MapFunction<Temperature, Tuple2<String, Double>>() {
              @Override
              public Tuple2<String, Double> map(Temperature input) throws Exception {
                return Tuple2.of(input.id, input.value);
              }
            })
            .keyBy(0)
            .timeWindow(Time.seconds(10))
            .reduce(new ReduceFunction<Tuple2<String, Double>>() {
              @Override
              public Tuple2<String, Double> reduce(Tuple2<String, Double> value1, Tuple2<String, Double> value2) throws Exception {
                System.out.println("reduce once");
                return Tuple2.of(value1.f0, Math.min(value1.f1, value2.f1));
              }
            })
            .print();

  }

  private final String ip = "localhost";

  private final Integer port = 7777;

}
