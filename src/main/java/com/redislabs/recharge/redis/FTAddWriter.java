package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.FTAddConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

import io.redisearch.client.Client;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FTAddWriter extends AbstractSearchWriter {

	private FTAddConfiguration add;

	public FTAddWriter(StringRedisTemplate template, RedisWriterConfiguration writer, Client client) {
		super(template, writer, client);
		this.add = writer.getSearch().getAdd();
	}

	@Override
	protected void write(Client client, String key, Map<String, Object> record) {
		double score = getScore(add, record);
		try {
			client.addDocument(key, score, record, add.isNoSave(), add.isReplace(), null);
		} catch (Exception e) {
			if ("Document already exists".equals(e.getMessage())) {
				log.debug(e.getMessage());
			} else {
				log.error("Could not add document: {}", e.getMessage());
			}
		}
	}

}
