package com.tank.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author tank198435163.com
 */
class String2StoreScalesMapping implements MapFunction<String, Tuple2<String, Integer>> {


  @Override
  public Tuple2<String, Integer> map(String value) throws Exception {
    final String[] arr = value.split(",");

    return Tuple2.of("total", Integer.parseInt(arr[1]));
  }
}
