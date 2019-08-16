package com.redislabs.riot.cli.redis;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.redis.JedisWriter;
import com.redislabs.riot.redis.LettuSearchConnector;
import com.redislabs.riot.redis.LettuceConnector;
import com.redislabs.riot.redis.LettuceWriter;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redisearch.AbstractLettuSearchItemWriter;

import io.lettuce.core.api.StatefulRedisConnection;
import picocli.CommandLine.Option;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisConnectionPoolOptions extends RedisConnectionOptions {

	private final Logger log = LoggerFactory.getLogger(RedisConnectionPoolOptions.class);

	@Option(names = "--max-total", description = "Maximum number of connections that can be allocated by the pool at a given time. Use a negative value for no limit (default: ${DEFAULT-VALUE})", paramLabel = "<integer>")
	private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
	@Option(names = "--min-idle", description = "Target for the minimum number of idle connections to maintain in the pool. This setting only has an effect if it is positive (default: ${DEFAULT-VALUE})", paramLabel = "<integer>")
	private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
	@Option(names = "--max-idle", description = "Maximum number of idle connections in the pool. Use a negative value to indicate an unlimited number of idle connections (default: ${DEFAULT-VALUE})", paramLabel = "<integer>")
	private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
	@Option(names = "--max-wait", description = "Maximum amount of time in milliseconds a connection allocation should block before throwing an exception when the pool is exhausted. Use a negative value to block indefinitely (default: ${DEFAULT-VALUE})")
	private long maxWait = GenericObjectPoolConfig.DEFAULT_MAX_WAIT_MILLIS;

	@SuppressWarnings("rawtypes")
	public <T extends GenericObjectPoolConfig> T configure(T poolConfig) {
		poolConfig.setMaxTotal(maxTotal);
		poolConfig.setMaxIdle(maxIdle);
		poolConfig.setMinIdle(minIdle);
		poolConfig.setMaxWaitMillis(maxWait);
		return poolConfig;
	}

	public JedisPool jedisPool() {
		JedisPoolConfig poolConfig = jedisPoolConfig();
		log.info("Creating Jedis connection to {}:{} with {}", host, port, poolConfig);
		return new JedisPool(poolConfig, host, port, connectionTimeout, socketTimeout, password, database, clientName);
	}

	private JedisPoolConfig jedisPoolConfig() {
		return configure(new JedisPoolConfig());
	}

	public GenericObjectPoolConfig<StatefulRedisConnection<String, String>> lettucePoolConfig() {
		return configure(new GenericObjectPoolConfig<>());
	}

	public LettuceConnector lettuceConnector() {
		return new LettuceConnector(clientResources(), redisUri(), lettucePoolConfig());
	}

	public LettuSearchConnector lettuSearchConnector() {
		return new LettuSearchConnector(clientResources(), redisUri(), lettucePoolConfig());
	}

	public ItemWriter<Map<String, Object>> writer(AbstractRedisItemWriter itemWriter) {
		if (client == RedisClient.jedis) {
			return new JedisWriter(jedisPool(), itemWriter);
		}
		return new LettuceWriter(lettuceConnector(), itemWriter);
	}

	public ItemWriter<Map<String, Object>> writer(AbstractLettuSearchItemWriter itemWriter) {
		return new LettuceWriter(lettuSearchConnector(), itemWriter);
	}

}
