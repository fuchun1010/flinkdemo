package com.tank.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import javax.annotation.Nonnull;
import java.util.Optional;


/**
 * @author tank198435163.com
 */
class AccIntegerProcessor extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Integer> {
 
  @Override
  public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Integer> out) throws Exception {
    Integer result = Optional.ofNullable(this.valueState.value()).orElse(0) + value.f1;
    this.valueState.update(result);
    out.collect(result);
  }

  @Override
  public void open(@Nonnull final Configuration parameters) throws Exception {
    ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor("result", TypeInformation.of(new TypeHint<Integer>() {
    }));
    this.valueState = this.getRuntimeContext().getState(valueStateDescriptor);
  }



  private ValueState<Integer> valueState;
}
