package com.tank.dm.ds;

import com.tank.dm.SqlProcessor;
import com.tank.dm.pojo.BasicOrder;
import com.tank.dm.pojo.Order;
import lombok.val;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.SQLException;
import java.util.Optional;

/**
 * @author tank198435163.com
 */
public class OmsDataSource extends RichSourceFunction<Order> {
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.sqlProcessor = new SqlProcessor();
  }

  @Override
  public void run(SourceContext<Order> ctx) throws Exception {
    if (!this.running) {
      return;
    }
    val sql = "select id,source_id as sourceId from orders limit 10";
    sqlProcessor.queryResult(sql, Optional.empty(), rs -> {
      Order order = new Order();
      BasicOrder basicOrder = new BasicOrder();
      try {
        Long id = rs.getLong("id");
        Long sourceId = rs.getLong("sourceId");
        basicOrder.setId(id).setSourceId(sourceId);
        order.setBasicOrder(basicOrder);
      } catch (SQLException ex) {
        ex.printStackTrace();
      }
      ctx.collect(order);
    });

  }

  @Override
  public void cancel() {
    this.running = false;
  }

  private SqlProcessor sqlProcessor;

  private volatile boolean running = true;
}
