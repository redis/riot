package com.redislabs.recharge.redis.aggregate.operation;

import com.redislabs.recharge.redis.aggregate.operation.reduce.Avg;
import com.redislabs.recharge.redis.aggregate.operation.reduce.Count;
import com.redislabs.recharge.redis.aggregate.operation.reduce.CountDistinct;
import com.redislabs.recharge.redis.aggregate.operation.reduce.CountDistinctish;
import com.redislabs.recharge.redis.aggregate.operation.reduce.FirstValue;
import com.redislabs.recharge.redis.aggregate.operation.reduce.Max;
import com.redislabs.recharge.redis.aggregate.operation.reduce.Min;
import com.redislabs.recharge.redis.aggregate.operation.reduce.Quantile;
import com.redislabs.recharge.redis.aggregate.operation.reduce.RandomSample;
import com.redislabs.recharge.redis.aggregate.operation.reduce.StdDev;
import com.redislabs.recharge.redis.aggregate.operation.reduce.Sum;
import com.redislabs.recharge.redis.aggregate.operation.reduce.ToList;

import lombok.Data;

@Data
public class ReduceFunction {

	private Avg avg;
	private Count count;
	private CountDistinct countDistinct;
	private CountDistinctish countDistinctish;
	private FirstValue firstValue;
	private Max max;
	private Min min;
	private Quantile quantile;
	private RandomSample randomSample;
	private StdDev stdDev;
	private Sum sum;
	private ToList toList;

}
