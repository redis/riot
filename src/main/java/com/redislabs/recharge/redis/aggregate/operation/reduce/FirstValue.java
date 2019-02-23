package com.redislabs.recharge.redis.aggregate.operation.reduce;

import com.redislabs.lettusearch.aggregate.reducer.By.Order;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class FirstValue extends PropertyReducer {

	private By by;

	@Data
	public static class By {
		private String property;
		private Order order;
	}

}
