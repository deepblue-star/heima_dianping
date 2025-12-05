package com.hmdp.mq.dto;

import lombok.Data;

@Data
public class CreateOrderDTO {
    private Long userId;
    private Long orderId;
    private Long voucherId;
}
