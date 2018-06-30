package com.redislabs.recharge.redis;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.redisearch.Schema;
import io.redisearch.client.Client;
import io.redisearch.client.Client.IndexOptions;
import io.redisearch.client.ClusterClient;
import redis.clients.jedis.exceptions.JedisDataException;

@Component
public class RediSearchWriter extends AbstractItemStreamItemWriter<HashItem> {

	private Logger log = LoggerFactory.getLogger(RediSearchWriter.class);

	@Autowired
	private RediSearchConfiguration config;

	private Client client;

	@Override
	public void open(ExecutionContext executionContext) {
		this.client = getClient();
	}

	private Client getClient() {
		if (config.isCluster()) {
			return new ClusterClient(getIndex(), getHost(), getPort(), getTimeout(), getPoolSize());
		}
		return new Client(getIndex(), getHost(), getPort(), getTimeout(), getPoolSize());
	}

	private String getHost() {
		if (config.getHost() == null) {
			return config.getHost();
		}
		return config.getHost();
	}

	private int getPort() {
		if (config.getPort() == null) {
			return config.getPort();
		}
		return config.getPort();
	}

	private String getIndex() {
		return config.getIndex();
	}

	private int getPoolSize() {
		if (config.getPoolSize() == null) {
			return config.getPoolSize();
		}
		return config.getPoolSize();
	}

	private int getTimeout() {
		if (config.getTimeout() == null) {
			return config.getTimeout();
		}
		return config.getTimeout();
	}

	public void createIndex(Schema schema, IndexOptions options) {
		client.createIndex(schema, options);
	}

	@Override
	public void write(List<? extends HashItem> items) throws Exception {
		for (HashItem item : items) {
			addDocument(item);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void addDocument(HashItem item) {
		try {
			client.addDocument(item.getKey(), 1.0, (Map) item.getValue(), true, false, null);
		} catch (JedisDataException e) {
			if ("Document already in index".equals(e.getMessage())) {
				log.debug(e.getMessage());
			}
			log.error("Could not add document: {}", e.getMessage());
		}
	}

}
