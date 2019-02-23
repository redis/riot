package com.redislabs.recharge.redis.aggregate.operation;

import lombok.Data;

@Data
public class LimitOperation {

	private long offset;
	private long num;
}