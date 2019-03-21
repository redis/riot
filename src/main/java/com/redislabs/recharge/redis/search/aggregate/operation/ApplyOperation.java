package com.redislabs.recharge.redis.search.aggregate.operation;

import lombok.Data;

@Data
public class ApplyOperation {

	private String expression;
	private String as;

}