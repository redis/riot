package com.redislabs.recharge.redisearch;

import com.redislabs.recharge.redis.suggest.SuggestConfiguration;
import com.redislabs.recharge.redis.suggest.SuggestWriter;
import com.redislabs.recharge.redisearch.add.AddConfiguration;
import com.redislabs.recharge.redisearch.add.AddWriter;
import com.redislabs.recharge.redisearch.aggregate.AggregateConfiguration;
import com.redislabs.recharge.redisearch.aggregate.AggregateWriter;
import com.redislabs.recharge.redisearch.search.SearchConfiguration;
import com.redislabs.recharge.redisearch.search.SearchWriter;

import lombok.Data;

@Data
public class RediSearchSinkConfiguration {
	private AddConfiguration add = new AddConfiguration();
	private SearchConfiguration search;
	private AggregateConfiguration aggregate;
	private SuggestConfiguration suggest;

	public RediSearchCommandWriter<?> writer() {
		if (search != null) {
			return new SearchWriter(search);
		}
		if (aggregate != null) {
			return new AggregateWriter(aggregate);
		}
		if (suggest != null) {
			return new SuggestWriter(suggest);
		}
		return new AddWriter(add);
	}
}
