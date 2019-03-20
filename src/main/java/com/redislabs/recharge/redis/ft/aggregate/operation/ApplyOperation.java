package com.redislabs.recharge.redis.ft.aggregate.operation;

import lombok.Data;

@Data
public class ApplyOperation {

	private String expression;
	private String as;

}