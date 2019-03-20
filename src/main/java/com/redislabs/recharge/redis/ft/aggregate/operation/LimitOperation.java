package com.redislabs.recharge.redis.ft.aggregate.operation;

import lombok.Data;

@Data
public class LimitOperation {

	private long offset;
	private long num;
}