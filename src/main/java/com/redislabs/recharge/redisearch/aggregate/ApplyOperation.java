package com.redislabs.recharge.redisearch.aggregate;

import lombok.Data;

@Data
public class ApplyOperation {

	private String expression;
	private String as;

}