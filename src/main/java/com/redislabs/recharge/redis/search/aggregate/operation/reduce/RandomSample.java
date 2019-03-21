package com.redislabs.recharge.redis.search.aggregate.operation.reduce;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class RandomSample extends PropertyReducer {

	private int size;

}
