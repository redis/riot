package com.redislabs.recharge.redisearch.aggregate;

import lombok.Data;

@Data
public class ReduceFunction {

	private PropertyReducer avg;
	private Reducer count;
	private PropertyReducer countDistinct;
	private PropertyReducer countDistinctish;
	private FirstValue firstValue;
	private PropertyReducer max;
	private PropertyReducer min;
	private Quantile quantile;
	private RandomSample randomSample;
	private PropertyReducer stdDev;
	private PropertyReducer sum;
	private PropertyReducer toList;

}
