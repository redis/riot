package com.redislabs.recharge.redisearch;

import com.redislabs.recharge.redisearch.aggregate.ApplyOperation;
import com.redislabs.recharge.redisearch.aggregate.FilterOperation;
import com.redislabs.recharge.redisearch.aggregate.GroupOperation;
import com.redislabs.recharge.redisearch.aggregate.LimitOperation;
import com.redislabs.recharge.redisearch.aggregate.SortOperation;

import lombok.Data;

@Data
public class AggregateOperation {

	private ApplyOperation apply;
	private FilterOperation filter;
	private LimitOperation limit;
	private SortOperation sort;
	private GroupOperation group;

}
