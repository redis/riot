package com.redislabs.recharge.redis;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.recharge.RechargeConfiguration.HashConfiguration;
import com.redislabs.recharge.RechargeConfiguration.NilConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;
import com.redislabs.recharge.redis.writers.AbstractRedisWriter;
import com.redislabs.recharge.redis.writers.FTAddWriter;
import com.redislabs.recharge.redis.writers.GeoAddWriter;
import com.redislabs.recharge.redis.writers.HMSetWriter;
import com.redislabs.recharge.redis.writers.LPushWriter;
import com.redislabs.recharge.redis.writers.NilWriter;
import com.redislabs.recharge.redis.writers.SAddWriter;
import com.redislabs.recharge.redis.writers.StringWriter;
import com.redislabs.recharge.redis.writers.SuggestionWriter;
import com.redislabs.recharge.redis.writers.XAddWriter;
import com.redislabs.recharge.redis.writers.ZAddWriter;

@Configuration
@SuppressWarnings("rawtypes")
public class RedisConfig {

	@Autowired
	private RediSearchClient client;

	public ItemWriter<Map> writer(List<RedisWriterConfiguration> redis) {
		if (redis.size() == 0) {
			return new NilWriter(new NilConfiguration());
		}
		if (redis.size() == 1) {
			return writer(redis.get(0)).setConnection(client.connect());
		}
		CompositeItemWriter<Map> composite = new CompositeItemWriter<>();
		composite.setDelegates(redis.stream().map(writer -> writer(writer).setConnection(client.connect()))
				.collect(Collectors.toList()));
		return composite;
	}

	private AbstractRedisWriter<?> writer(RedisWriterConfiguration writer) {
		if (writer.getSet() != null) {
			return new SAddWriter(writer.getSet());
		}
		if (writer.getGeo() != null) {
			return new GeoAddWriter(writer.getGeo());
		}
		if (writer.getList() != null) {
			return new LPushWriter(writer.getList());
		}
		if (writer.getSearch() != null) {
			return new FTAddWriter(writer.getSearch());
		}
		if (writer.getSet() != null) {
			return new SAddWriter(writer.getSet());
		}
		if (writer.getString() != null) {
			return new StringWriter(writer.getString());
		}
		if (writer.getSuggest() != null) {
			return new SuggestionWriter(writer.getSuggest());
		}
		if (writer.getZset() != null) {
			return new ZAddWriter(writer.getZset());
		}
		if (writer.getStream() != null) {
			return new XAddWriter(writer.getStream());
		}
		if (writer.getHash() != null) {
			return new HMSetWriter(writer.getHash());
		}
		return new HMSetWriter(new HashConfiguration());
	}

}
