package com.redislabs.recharge.batch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.redislabs.recharge.config.Recharge;
import com.redislabs.recharge.config.RediSearch;

import io.redisearch.client.Client;
import io.redisearch.client.ClusterClient;
import redis.clients.jedis.exceptions.JedisDataException;

@Component
public class RediSearchWriter extends ItemStreamSupport implements ItemWriter<Map<String, String>> {

	private Logger log = LoggerFactory.getLogger(RediSearchWriter.class);

	@Autowired
	private Recharge config;

	private String keyPrefix;

	private String[] keyFields;

	private String keySeparator;

	private Client client;

	@Override
	public void open(ExecutionContext executionContext) {
		this.keySeparator = config.getKey().getSeparator();
		this.keyPrefix = config.getKey().getPrefix() + keySeparator;
		this.keyFields = config.getKey().getFields();
	}

	public void open() {
		this.client = getClient();
	}

	private Client getClient() {
		RediSearch redisearch = config.getRedisearch();
		if (redisearch.isCluster()) {
			return new ClusterClient(redisearch.getIndex(), redisearch.getHost(), redisearch.getPort(),
					redisearch.getTimeout(), redisearch.getPoolSize());
		}
		return new Client(redisearch.getIndex(), redisearch.getHost(), redisearch.getPort(), redisearch.getTimeout(),
				redisearch.getPoolSize());
	}

	@Override
	public void write(List<? extends Map<String, String>> items) throws Exception {
		for (Map<String, String> item : items) {
			Map<String, Object> fields = new HashMap<String, Object>();
			fields.putAll(item);
			String key = getKey(item);
			try {
				client.addDocument(key, 1.0, fields, true, false, null);
			} catch (JedisDataException e) {
				if ("Document already in index".equals(e.getMessage())) {
					log.debug(e.getMessage());
				}
				log.error("Could not add document: {}", e.getMessage());
			}
		}
	}

	private String getKey(Map<String, String> item) {
		String key = keyPrefix;
		for (int i = 0; i < keyFields.length - 1; i++) {
			key += item.get(keyFields[i]) + keySeparator;
		}
		key += item.get(keyFields[keyFields.length - 1]);
		return key;
	}
}
