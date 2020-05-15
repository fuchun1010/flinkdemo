package com.tank.state;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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

    env.enableCheckpointing(interval);
    final CheckpointConfig checkpointConfig = env.getCheckpointConfig();
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    checkpointConfig.setCheckpointTimeout(maxAllowedTimeOutForCheckPoint);

    accumulateInt(env, ip, port);

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
            .uid("string2StoreScalesMapping")
            .keyBy(0)
            .process(new AccIntegerProcessor())
            .uid("accIntegerProcessor")
            .print()
            .uid("print");
  }

  private Long interval = 1000L;

  private Long maxAllowedTimeOutForCheckPoint = 6000L;


}
