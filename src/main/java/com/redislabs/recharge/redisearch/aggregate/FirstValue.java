package com.redislabs.recharge.redisearch.aggregate;

import com.redislabs.lettusearch.aggregate.reducer.By;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class FirstValue extends PropertyReducer {

	private By by;

}
