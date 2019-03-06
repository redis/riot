package com.redislabs.recharge.redis;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration;
import com.redislabs.recharge.RechargeException;
import com.redislabs.recharge.redis.aggregate.AggregateWriter;
import com.redislabs.recharge.redis.geo.GeoAddWriter;
import com.redislabs.recharge.redis.hash.HMSetWriter;
import com.redislabs.recharge.redis.hash.HashConfiguration;
import com.redislabs.recharge.redis.list.LPushWriter;
import com.redislabs.recharge.redis.search.FTAddWriter;
import com.redislabs.recharge.redis.set.SAddWriter;
import com.redislabs.recharge.redis.set.SRemWriter;
import com.redislabs.recharge.redis.stream.XAddWriter;
import com.redislabs.recharge.redis.string.StringWriter;
import com.redislabs.recharge.redis.suggest.SuggestWriter;
import com.redislabs.recharge.redis.zset.ZAddWriter;

@Configuration
public class RedisConfig {

	@Autowired
	private RechargeConfiguration config;
	@Autowired
	private GenericObjectPool<StatefulRediSearchConnection<String, String>> pool;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;

	public RedisWriter<?> writer() throws RechargeException {
		RedisConfiguration redis = config.getRedis();
		if (redis.getSet() != null) {
			switch (redis.getSet().getCommand()) {
			case sadd:
				return new SAddWriter(redis.getSet(), pool);
			case srem:
				return new SRemWriter(redis.getSet(), pool);
			}
		}
		if (redis.getGeo() != null) {
			return new GeoAddWriter(redis.getGeo(), pool);
		}
		if (redis.getList() != null) {
			return new LPushWriter(redis.getList(), pool);
		}
		if (redis.getAggregate() != null) {
			return new AggregateWriter(redis.getAggregate(), pool);
		}
		if (redis.getSearch() != null) {
			return new FTAddWriter(redis.getSearch(), pool);
		}
		if (redis.getSet() != null) {
			return new SAddWriter(redis.getSet(), pool);
		}
		if (redis.getString() != null) {
			return new StringWriter(redis.getString(), pool);
		}
		if (redis.getSuggest() != null) {
			return new SuggestWriter(redis.getSuggest(), pool);
		}
		if (redis.getZset() != null) {
			return new ZAddWriter(redis.getZset(), pool);
		}
		if (redis.getStream() != null) {
			return new XAddWriter(redis.getStream(), pool);
		}
		if (redis.getHash() == null) {
			redis.setHash(new HashConfiguration());
		}
		return new HMSetWriter(redis.getHash(), pool);
	}

	public RedisReader reader() {
		ScanConfiguration scan = config.getRedis().getScan();
		if (scan == null) {
			scan = new ScanConfiguration();
		}
		RedisReader reader = new RedisReader();
		reader.setConnection(connection);
		reader.setConfig(scan);
		return reader;
	}

}
