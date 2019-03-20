package com.redislabs.recharge.redis.ft;

import com.redislabs.recharge.redis.ft.add.AddConfiguration;
import com.redislabs.recharge.redis.ft.add.AddWriter;
import com.redislabs.recharge.redis.ft.aggregate.AggregateConfiguration;
import com.redislabs.recharge.redis.ft.aggregate.AggregateWriter;
import com.redislabs.recharge.redis.ft.search.SearchConfiguration;
import com.redislabs.recharge.redis.ft.search.SearchWriter;
import com.redislabs.recharge.redis.suggest.SuggestConfiguration;
import com.redislabs.recharge.redis.suggest.SuggestWriter;

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
