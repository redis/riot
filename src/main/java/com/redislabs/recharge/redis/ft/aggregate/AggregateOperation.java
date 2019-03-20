package com.redislabs.recharge.redis.ft.aggregate;

import com.redislabs.recharge.redis.ft.aggregate.operation.ApplyOperation;
import com.redislabs.recharge.redis.ft.aggregate.operation.FilterOperation;
import com.redislabs.recharge.redis.ft.aggregate.operation.GroupOperation;
import com.redislabs.recharge.redis.ft.aggregate.operation.LimitOperation;
import com.redislabs.recharge.redis.ft.aggregate.operation.SortOperation;

import lombok.Data;

@Data
public class AggregateOperation {

	private ApplyOperation apply;
	private FilterOperation filter;
	private LimitOperation limit;
	private SortOperation sort;
	private GroupOperation group;

}
