package com.tank.dm;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author tank198435163.com
 */
public final class SpecialTuple3 {

  public static <T> Tuple3<Long, T, Long> create(Long id, T body, Long millionSeconds) {
    return Tuple3.of(id, body, millionSeconds);
  }

}
