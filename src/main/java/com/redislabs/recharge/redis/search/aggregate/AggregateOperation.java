package com.redislabs.recharge.redis.search.aggregate;

import com.redislabs.recharge.redis.search.aggregate.operation.ApplyOperation;
import com.redislabs.recharge.redis.search.aggregate.operation.FilterOperation;
import com.redislabs.recharge.redis.search.aggregate.operation.GroupOperation;
import com.redislabs.recharge.redis.search.aggregate.operation.LimitOperation;
import com.redislabs.recharge.redis.search.aggregate.operation.SortOperation;

import lombok.Data;

@Data
public class AggregateOperation {

	private ApplyOperation apply;
	private FilterOperation filter;
	private LimitOperation limit;
	private SortOperation sort;
	private GroupOperation group;

}
