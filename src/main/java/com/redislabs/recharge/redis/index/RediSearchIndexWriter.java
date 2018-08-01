package com.redislabs.recharge.redis.index;

import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RediSearchField;

import io.redisearch.Schema;
import io.redisearch.Schema.Field;
import io.redisearch.client.Client;
import io.redisearch.client.Client.IndexOptions;
import io.redisearch.client.ClusterClient;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.exceptions.JedisDataException;

@Slf4j
public class RediSearchIndexWriter extends AbstractIndexWriter {

	private Entry<String, IndexConfiguration> index;
	private RedisProperties redisConfig;
	private Client client;

	public RediSearchIndexWriter(StringRedisTemplate template, Entry<String, EntityConfiguration> entity,
			Entry<String, IndexConfiguration> index, RedisProperties redisConfig) {
		super(template, entity, index);
		this.index = index;
		this.redisConfig = redisConfig;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		this.client = getClient();
		if (!index.getValue().getSchema().isEmpty()) {
			Schema schema = new Schema();
			index.getValue().getSchema().entrySet().forEach(entry -> schema.addField(getField(entry)));
			client.createIndex(schema, Client.IndexOptions.Default());
		}
	}

	private Field getField(Entry<String, RediSearchField> entry) {
		String name = entry.getKey();
		RediSearchField field = entry.getValue();
		return new Field(name, field.getType(), field.isSortable(), field.isNoIndex());
	}

	private Client getClient() {
		IndexConfiguration config = index.getValue();
		if (config.isCluster()) {
			return new ClusterClient(getIndex(), getHost(), getPort(), config.getTimeout(), config.getPoolSize());
		}
		return new Client(getIndex(), getHost(), getPort(), config.getTimeout(), config.getPoolSize());
	}

	private String getHost() {
		IndexConfiguration config = index.getValue();
		if (config.getHost() == null) {
			return redisConfig.getHost();
		}
		return config.getHost();
	}

	private int getPort() {
		IndexConfiguration config = index.getValue();
		if (config.getPort() == null) {
			return redisConfig.getPort();
		}
		return config.getPort();
	}

	private String getIndex() {
		return index.getKey();
	}

	public void createIndex(Schema schema, IndexOptions options) {
		client.createIndex(schema, options);
	}

	@Override
	protected void write(StringRedisConnection conn, Map<String, Object> record, String id, String key,
			String indexKey) {
		try {
			client.addHash(key, 1.0, true);
		} catch (JedisDataException e) {
			if ("Document already in index".equals(e.getMessage())) {
				log.debug(e.getMessage());
			}
			log.error("Could not add document: {}", e.getMessage());
		}
	}

	@Override
	protected String getDefaultKeyspace() {
		return "search";
	}

}
