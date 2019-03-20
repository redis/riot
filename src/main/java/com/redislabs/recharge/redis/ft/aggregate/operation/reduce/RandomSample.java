package com.redislabs.recharge.redis.ft.aggregate.operation.reduce;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class RandomSample extends PropertyReducer {

	private int size;

}
