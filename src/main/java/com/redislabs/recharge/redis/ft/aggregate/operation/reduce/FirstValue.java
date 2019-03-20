package com.redislabs.recharge.redis.ft.aggregate.operation.reduce;

import com.redislabs.lettusearch.aggregate.reducer.By;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class FirstValue extends PropertyReducer {

	private By by;

}
