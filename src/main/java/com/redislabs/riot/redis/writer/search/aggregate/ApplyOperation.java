package com.redislabs.riot.redis.writer.search.aggregate;

import lombok.Data;

@Data
public class ApplyOperation {

	private String expression;
	private String as;

}