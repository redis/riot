package com.redislabs.recharge.redis.aggregate.operation;

import lombok.Data;

@Data
public class ApplyOperation {

	private String expression;
	private String as;

}