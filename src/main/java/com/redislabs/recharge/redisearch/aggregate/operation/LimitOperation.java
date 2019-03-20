package com.redislabs.recharge.redisearch.aggregate.operation;

import lombok.Data;

@Data
public class LimitOperation {

	private long offset;
	private long num;
}