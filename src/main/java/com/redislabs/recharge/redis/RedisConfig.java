package com.redislabs.recharge.redis;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration;
import com.redislabs.recharge.RechargeException;
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

	public List<RedisWriter<?>> writers() throws RechargeException {
		if (config.getRedis().size() == 0) {
			RedisConfiguration writerConfig = new RedisConfiguration();
			writerConfig.setHash(new HashConfiguration());
			config.getRedis().add(writerConfig);
		}
		return config.getRedis().stream().map(redis -> writer(redis, pool)).collect(Collectors.toList());
	}

	private RedisWriter<?> writer(RedisConfiguration writerConfig,
			GenericObjectPool<StatefulRediSearchConnection<String, String>> pool) {
		if (writerConfig.getSet() != null) {
			switch (writerConfig.getSet().getCommand()) {
			case sadd:
				return new SAddWriter(sanitize(writerConfig.getSet()), pool);
			case srem:
				return new SRemWriter(sanitize(writerConfig.getSet()), pool);
			}
		}
		if (writerConfig.getGeo() != null) {
			return new GeoAddWriter(sanitize(writerConfig.getGeo()), pool);
		}
		if (writerConfig.getList() != null) {
			return new LPushWriter(sanitize(writerConfig.getList()), pool);
		}
		if (writerConfig.getSearch() != null) {
			return new FTAddWriter(sanitize(writerConfig.getSearch()), pool);
		}
		if (writerConfig.getSet() != null) {
			return new SAddWriter(sanitize(writerConfig.getSet()), pool);
		}
		if (writerConfig.getString() != null) {
			return new StringWriter(sanitize(writerConfig.getString()), pool);
		}
		if (writerConfig.getSuggest() != null) {
			return new SuggestWriter(sanitize(writerConfig.getSuggest()), pool);
		}
		if (writerConfig.getZset() != null) {
			return new ZAddWriter(sanitize(writerConfig.getZset()), pool);
		}
		if (writerConfig.getStream() != null) {
			return new XAddWriter(sanitize(writerConfig.getStream()), pool);
		}
		if (writerConfig.getHash() != null) {
			return new HMSetWriter(sanitize(writerConfig.getHash()), pool);
		}
		return new HMSetWriter(sanitize(new HashConfiguration()), pool);
	}

	private <T extends RedisDataStructureConfiguration> T sanitize(T redis) {
		if (redis.getKeyspace() == null) {
			redis.setKeyspace(config.getName());
		}
		if (redis instanceof CollectionRedisConfiguration) {
			CollectionRedisConfiguration collection = (CollectionRedisConfiguration) redis;
			if (collection.getFields().length == 0 && config.getFields().length > 0) {
				collection.setFields(new String[] { config.getFields()[0] });
			}
		} else {
			if (redis.getKeys().length == 0 && config.getFields().length > 0) {
				redis.setKeys(new String[] { config.getFields()[0] });
			}
		}
		return redis;
	}

}
