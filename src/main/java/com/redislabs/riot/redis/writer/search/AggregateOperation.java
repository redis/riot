package com.redislabs.riot.redis.writer.search;

import com.redislabs.riot.redis.writer.search.aggregate.ApplyOperation;
import com.redislabs.riot.redis.writer.search.aggregate.FilterOperation;
import com.redislabs.riot.redis.writer.search.aggregate.GroupOperation;
import com.redislabs.riot.redis.writer.search.aggregate.LimitOperation;
import com.redislabs.riot.redis.writer.search.aggregate.SortOperation;

import lombok.Data;

@Data
public class AggregateOperation {

	private ApplyOperation apply;
	private FilterOperation filter;
	private LimitOperation limit;
	private SortOperation sort;
	private GroupOperation group;

}
