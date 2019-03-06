package com.redislabs.recharge.redis;

import com.redislabs.recharge.redis.aggregate.AggregateConfiguration;
import com.redislabs.recharge.redis.geo.GeoConfiguration;
import com.redislabs.recharge.redis.hash.HashConfiguration;
import com.redislabs.recharge.redis.list.ListConfiguration;
import com.redislabs.recharge.redis.search.SearchConfiguration;
import com.redislabs.recharge.redis.set.SetConfiguration;
import com.redislabs.recharge.redis.stream.StreamConfiguration;
import com.redislabs.recharge.redis.string.StringConfiguration;
import com.redislabs.recharge.redis.suggest.SuggestConfiguration;
import com.redislabs.recharge.redis.zset.ZSetConfiguration;

import lombok.Data;

@Data
public class RedisConfiguration {
	private StringConfiguration string;
	private HashConfiguration hash;
	private ListConfiguration list;
	private SetConfiguration set;
	private ZSetConfiguration zset;
	private GeoConfiguration geo;
	private SearchConfiguration search;
	private AggregateConfiguration aggregate;
	private SuggestConfiguration suggest;
	private StreamConfiguration stream;
	private ScanConfiguration scan;

	public void setKeyspace(String keyspace) {
		setKeyspace(keyspace, string, hash, list, set, zset, geo, search, aggregate, suggest, stream);
	}

	private void setKeyspace(String keyspace, DataStructureConfiguration... configs) {
		for (DataStructureConfiguration config : configs) {
			if (config == null) {
				continue;
			}
			if (config.getKeyspace() == null) {
				config.setKeyspace(keyspace);
			}
		}
	}

	public void setCollectionFields(String... fields) {
		if (fields.length > 0) {
			setFields(fields[0], list, set, zset, geo);
		}
	}

	private void setFields(String field, CollectionRedisConfiguration... collections) {
		for (CollectionRedisConfiguration collection : collections) {
			if (collection == null) {
				continue;
			}
			if (collection.getFields().length == 0) {
				collection.setFields(new String[] { field });
			}
		}

	}
}
