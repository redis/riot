package com.redislabs.recharge.redis.aggregate.operation.reduce;

import com.redislabs.lettusearch.aggregate.reducer.By.Order;

import lombok.Data;

@Data
public class By {
	private String property;
	private Order order;
}