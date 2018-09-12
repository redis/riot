package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

import io.redisearch.client.Client;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FTAddHashWriter extends AbstractSearchWriter {

	public FTAddHashWriter(StringRedisTemplate template, RedisWriterConfiguration writer, Client client) {
		super(template, writer, client);
	}

	@Override
	protected void write(Client client, String key, Map<String, Object> record) {
		double score = getScore(record);
		try {
			client.addHash(key, score, getSearch().isReplace());
		} catch (Exception e) {
			if ("Document already exists".equals(e.getMessage())) {
				log.debug(e.getMessage());
			} else {
				log.error("Could not add hash: {}", key, e);
			}
		}
	}

}
