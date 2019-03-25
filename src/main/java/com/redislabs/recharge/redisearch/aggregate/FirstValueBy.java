package com.redislabs.recharge.redisearch.aggregate;

import com.redislabs.lettusearch.aggregate.reducer.By.Order;

import lombok.Data;

@Data
public class FirstValueBy {

	private String property;
	private Order order;

}