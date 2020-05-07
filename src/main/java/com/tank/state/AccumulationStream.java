package com.tank.state;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import javax.annotation.Nonnull;

/**
 * @author tank198435163.com
 */
public class AccumulationStream {

  public void processIntegerStream(@Nonnull final String ip,
                                   @Nonnull final Integer port) throws Exception {

    System.out.println("AccumulationStream processIntegerStream start");

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    accumulateInt(env, ip, port);

//    env.executeAsync("processIntegerStream job")
//            .getJobStatus()
//            .whenComplete((status, err) -> {
//              System.out.println(String.format("processIntegerStream name:[%s]", status.name()));
//            });
    env.execute("processIntegerStream job");
  }

  public AccumulationStream() {
  }

  private static void accumulateInt(@Nonnull final StreamExecutionEnvironment env,
                                    @Nonnull final String ip,
                                    @Nonnull final Integer port) {

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    env.getConfig().disableGenericTypes();

    env.socketTextStream(ip, port)
            .map(new String2StoreScalesMapping())
            .keyBy(0)
            .process(new AccIntegerProcessor())
            .print();
  }


}
