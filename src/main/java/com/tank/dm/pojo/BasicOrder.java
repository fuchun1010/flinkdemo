package com.tank.dm.pojo;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
public class BasicOrder {

  public Long id;

  public Long sourceId;

  public String orderSn;

  public String orderType;

  public String orderBrand;

  public String orderTakeType;

  public String channelKey;

  public String operatorStatus;

  public String channelOrderSn;

  public String channelOrderId;

  public String serial_no;

  public String store_code;

  public String store_name;

  public String warehouse_no;

  public String customer_account;

  public String customer_mobile;

  public String region_code;

  public String city_code;

  public int status;

  public int payment_status;

  public String order_status;

  public String process_status;

  public String pickup_code;

  public String ticket_no;
}
