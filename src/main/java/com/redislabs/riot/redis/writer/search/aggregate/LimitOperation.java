package com.redislabs.riot.redis.writer.search.aggregate;

import lombok.Data;

@Data
public class LimitOperation {

	private long offset;
	private long num;
}