package com.redislabs.recharge.redis.aggregate.operation.reduce;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class Avg extends AbstractReducer {

	private String property;

}
