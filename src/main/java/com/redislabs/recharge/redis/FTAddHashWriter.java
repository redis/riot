package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.FTAddHashConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

import io.redisearch.client.Client;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FTAddHashWriter extends AbstractSearchWriter {

	private FTAddHashConfiguration addHash;

	public FTAddHashWriter(StringRedisTemplate template, RedisWriterConfiguration writer, Client client) {
		super(template, writer, client);
		this.addHash = writer.getSearch().getAddHash();
	}

	@Override
	protected void write(Client client, String key, Map<String, Object> record) {
		double score = getScore(addHash, record);
		try {
			client.addHash(key, score, addHash.isReplace());
		} catch (Exception e) {
			if ("Document already exists".equals(e.getMessage())) {
				log.debug(e.getMessage());
			} else {
				log.error("Could not add hash: {}", key, e);
			}
		}
	}

}
