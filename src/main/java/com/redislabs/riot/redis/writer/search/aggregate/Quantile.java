package com.redislabs.riot.redis.writer.search.aggregate;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class Quantile extends PropertyReducer {

	private double quantile;

}
