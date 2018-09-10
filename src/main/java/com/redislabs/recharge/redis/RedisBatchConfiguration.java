package com.redislabs.recharge.redis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

import io.redisearch.client.Client;

@Component
public class RedisBatchConfiguration {

	@Autowired
	private StringRedisTemplate template;

	@Autowired
	private RediSearchClientConfiguration searchClient;

	public AbstractRedisWriter getWriter(RedisWriterConfiguration writer) {
		if (writer.getNil() != null) {
			return new NilWriter(template, writer);
		}
		if (writer.getString() != null) {
			return new StringWriter(template, writer);
		}
		if (writer.getGeo() != null) {
			return new GeoAddWriter(template, writer);
		}
		if (writer.getList() != null) {
			return new LPushWriter(template, writer);
		}
		if (writer.getSearch() != null) {
			Client client = searchClient.getClient(writer.getSearch().getIndex());
			if (writer.getSearch().getAdd() != null) {
				return new FTAddWriter(template, writer, client);
			}
			return new FTAddHashWriter(template, writer, client);
		}
		if (writer.getSuggest() != null) {
			Client client = searchClient.getClient(writer.getSearch().getIndex());
			return new SugAddWriter(template, writer, client);
		}
		if (writer.getZset() != null) {
			return new ZAddWriter(template, writer);
		}
		if (writer.getSet() != null) {
			return new SAddWriter(template, writer);
		}
		if (writer.getHash() != null) {
			if (writer.getHash().getIncrBy() != null) {
				return new HIncrByWriter(template, writer);
			}
		}
		return new HMSetWriter(template, writer);
	}

}
