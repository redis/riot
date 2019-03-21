package com.redislabs.recharge.redis.search.aggregate.operation;

import com.redislabs.recharge.redis.search.aggregate.operation.reduce.FirstValue;
import com.redislabs.recharge.redis.search.aggregate.operation.reduce.PropertyReducer;
import com.redislabs.recharge.redis.search.aggregate.operation.reduce.Quantile;
import com.redislabs.recharge.redis.search.aggregate.operation.reduce.RandomSample;
import com.redislabs.recharge.redis.search.aggregate.operation.reduce.Reducer;

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
