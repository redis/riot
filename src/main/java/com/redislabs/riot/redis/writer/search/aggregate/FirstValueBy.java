package com.redislabs.riot.redis.writer.search.aggregate;

import com.redislabs.lettusearch.aggregate.Order;

import lombok.Data;

@Data
public class FirstValueBy {

	private String property;
	private Order order;

}