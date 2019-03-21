package com.redislabs.recharge.redis.search.aggregate.operation.reduce;

import com.redislabs.lettusearch.aggregate.reducer.By.Order;

import lombok.Data;

@Data
public class FirstValueBy {

	private String property;
	private Order order;

}