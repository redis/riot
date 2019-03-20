package com.redislabs.recharge.redis;

import com.redislabs.recharge.redis.ft.RediSearchSinkConfiguration;
import com.redislabs.recharge.redis.geo.GeoAddWriter;
import com.redislabs.recharge.redis.geo.GeoConfiguration;
import com.redislabs.recharge.redis.hash.HMSetWriter;
import com.redislabs.recharge.redis.hash.HashConfiguration;
import com.redislabs.recharge.redis.list.LPushWriter;
import com.redislabs.recharge.redis.list.ListConfiguration;
import com.redislabs.recharge.redis.set.SAddWriter;
import com.redislabs.recharge.redis.set.SRemWriter;
import com.redislabs.recharge.redis.set.SetConfiguration;
import com.redislabs.recharge.redis.stream.StreamConfiguration;
import com.redislabs.recharge.redis.stream.XAddWriter;
import com.redislabs.recharge.redis.string.StringConfiguration;
import com.redislabs.recharge.redis.string.StringWriter;
import com.redislabs.recharge.redis.zset.ZAddWriter;
import com.redislabs.recharge.redis.zset.ZSetConfiguration;

import lombok.Data;

@Data
public class RedisSinkConfiguration {
	private StringConfiguration string;
	private HashConfiguration hash = new HashConfiguration();
	private ListConfiguration list;
	private SetConfiguration set;
	private ZSetConfiguration zset;
	private GeoConfiguration geo;
	private StreamConfiguration stream;
	private RediSearchSinkConfiguration ft;

	public void setKeyspace(String keyspace) {
		setKeyspace(keyspace, string, hash, list, set, zset, geo, stream);
	}

	private void setKeyspace(String keyspace, RedisCommandConfiguration... configs) {
		for (RedisCommandConfiguration config : configs) {
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

	public PipelineRedisWriter writer() {
		if (ft != null) {
			return ft.writer();
		}
		if (set != null) {
			switch (set.getCommand()) {
			case sadd:
				return new SAddWriter(set);
			case srem:
				return new SRemWriter(set);
			}
		}
		if (geo != null) {
			return new GeoAddWriter(geo);
		}
		if (list != null) {
			return new LPushWriter(list);
		}
		if (string != null) {
			return new StringWriter(string);
		}
		if (zset != null) {
			return new ZAddWriter(zset);
		}
		if (stream != null) {
			return new XAddWriter(stream);
		}
		return new HMSetWriter(hash);
	}
}
