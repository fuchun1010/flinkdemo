package com.tank.dm.pojo;

import com.tank.dm.SpecialTuple3;
import lombok.*;
import lombok.experimental.Accessors;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;

/**
 * @author tank198435163.com
 */
@Setter
@Getter
@Accessors(chain = true)
@ToString(of = {"id", "sourceId"})
public class Order implements Serializable {

  private BasicOrder basicOrder;

  public Tuple3<Long, Order, Long> toTuple3() {
    return SpecialTuple3.create(basicOrder.getId(), this, System.currentTimeMillis());
  }

}
