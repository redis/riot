package com.redislabs.recharge.redis.index;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.Entity;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RediSearchConfiguration;

import io.redisearch.Schema;
import io.redisearch.client.Client;
import io.redisearch.client.Client.IndexOptions;
import io.redisearch.client.ClusterClient;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.exceptions.JedisDataException;

@Slf4j
public class RediSearchIndexWriter extends AbstractIndexWriter {

	private RediSearchConfiguration config;
	private RedisProperties redisConfig;
	private Client client;

	public RediSearchIndexWriter(EntityConfiguration entityConfig, StringRedisTemplate template,
			IndexConfiguration config, RedisProperties redisConfig) {
		super(entityConfig, template, config);
		
		// TODO
		this.redisConfig = redisConfig;
	}

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
			return redisConfig.getHost();
		}
		return config.getHost();
	}

	private int getPort() {
		if (config.getPort() == null) {
			return redisConfig.getPort();
		}
		return config.getPort();
	}

	private String getIndex() {
		return config.getIndex();
	}

	private int getPoolSize() {
		if (config.getPoolSize() == null) {
			return redisConfig.getJedis().getPool().getMaxActive();
		}
		return config.getPoolSize();
	}

	private int getTimeout() {
		if (config.getTimeout() == null) {
			return (int) redisConfig.getJedis().getPool().getMaxWait().toMillis();
		}
		return config.getTimeout();
	}

	public void createIndex(Schema schema, IndexOptions options) {
		client.createIndex(schema, options);
	}

	@Override
	protected void write(StringRedisConnection conn, Entity entity, String id, String indexKey) {
		try {
			client.addDocument(indexKey, 1.0, entity.getFields(), true, false, null);
		} catch (JedisDataException e) {
			if ("Document already in index".equals(e.getMessage())) {
				log.debug(e.getMessage());
			}
			log.error("Could not add document: {}", e.getMessage());
		}
	}

}
