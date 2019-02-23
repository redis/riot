package com.redislabs.recharge.redis.aggregate;

import com.redislabs.recharge.redis.aggregate.operation.ApplyOperation;
import com.redislabs.recharge.redis.aggregate.operation.FilterOperation;
import com.redislabs.recharge.redis.aggregate.operation.GroupOperation;
import com.redislabs.recharge.redis.aggregate.operation.LimitOperation;
import com.redislabs.recharge.redis.aggregate.operation.SortOperation;

import lombok.Data;

@Data
public class AggregateOperation {

	private ApplyOperation apply;
	private FilterOperation filter;
	private LimitOperation limit;
	private SortOperation sort;
	private GroupOperation group;

}
