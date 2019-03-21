package com.redislabs.recharge.redis.search;

import com.redislabs.recharge.redis.search.add.AddConfiguration;
import com.redislabs.recharge.redis.search.add.AddWriter;
import com.redislabs.recharge.redis.search.aggregate.AggregateConfiguration;
import com.redislabs.recharge.redis.search.aggregate.AggregateWriter;
import com.redislabs.recharge.redis.search.search.SearchConfiguration;
import com.redislabs.recharge.redis.search.search.SearchWriter;
import com.redislabs.recharge.redis.search.suggest.SuggestConfiguration;
import com.redislabs.recharge.redis.search.suggest.SuggestWriter;

import lombok.Data;

@Data
public class RediSearchConfiguration {
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
