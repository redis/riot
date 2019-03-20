package com.redislabs.recharge.redisearch.aggregate;

import com.redislabs.recharge.redisearch.aggregate.operation.ApplyOperation;
import com.redislabs.recharge.redisearch.aggregate.operation.FilterOperation;
import com.redislabs.recharge.redisearch.aggregate.operation.GroupOperation;
import com.redislabs.recharge.redisearch.aggregate.operation.LimitOperation;
import com.redislabs.recharge.redisearch.aggregate.operation.SortOperation;

import lombok.Data;

@Data
public class AggregateOperation {

	private ApplyOperation apply;
	private FilterOperation filter;
	private LimitOperation limit;
	private SortOperation sort;
	private GroupOperation group;

}
