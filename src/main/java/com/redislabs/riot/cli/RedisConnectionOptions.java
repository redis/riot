package com.redislabs.riot.cli;

import java.time.Duration;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.DefaultClientResources.Builder;
import io.lettuce.core.support.ConnectionPoolSupport;
import picocli.CommandLine.Option;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class RedisConnectionOptions {

	public enum RedisDriver {
		jedis, lettuce
	}

	@Option(names = "--host", description = "Redis server host")
	private String host = "localhost";
	@Option(names = "--port", description = "Redis server port", paramLabel = "<integer>")
	private int port = RedisURI.DEFAULT_REDIS_PORT;
	@Option(names = "--command-timeout", description = "Redis command timeout in seconds for synchronous command execution", paramLabel = "<seconds>")
	private long commandTimeout = RedisURI.DEFAULT_TIMEOUT;
	@Option(names = "--connection-timeout", description = "Redis connect timeout in milliseconds", paramLabel = "<millis>")
	private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
	@Option(names = "--socket-timeout", description = "Redis socket timeout in milliseconds", paramLabel = "<millis>")
	private int socketTimeout = Protocol.DEFAULT_TIMEOUT;
	@Option(names = "--auth", arity = "0..1", interactive = true, description = "Redis database login password", paramLabel = "<string>")
	private String password;
	@Option(names = "--pool-max-idle", description = "Maximum number of idle connections in the pool. Use a negative value to indicate an unlimited number of idle connections", paramLabel = "<integer>")
	private int maxIdle = 8;
	@Option(names = "--pool-min-idle", description = "Target for the minimum number of idle connections to maintain in the pool. This setting only has an effect if it is positive", paramLabel = "<integer>")
	private int minIdle = 0;
	@Option(names = "--pool", description = "Maximum number of connections that can be allocated by the pool at a given time. Use a negative value for no limit", paramLabel = "<integer>")
	private int maxTotal = 8;
	@Option(names = "--pool-max-wait", description = "Maximum amount of time in milliseconds a connection allocation should block before throwing an exception when the pool is exhausted. Use a negative value to block indefinitely (default)")
	private long maxWait = -1L;
	@Option(names = "--db", description = "Redis database number. Databases are only available for Redis Standalone and Redis Master/Slave")
	private int database = 0;
	@Option(names = "--client", description = "Redis client name", paramLabel = "<string>")
	private String clientName;
	@Option(names = "--driver", description = "Redis driver: ${COMPLETION-CANDIDATES}")
	private RedisDriver driver = RedisDriver.lettuce;
	@Option(names = "--metrics", description = "Show metrics (only works with Lettuce driver)")
	private boolean showMetrics;

	public JedisPool jedisPool() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(maxIdle);
		config.setMaxTotal(maxTotal);
		config.setMaxWaitMillis(maxWait);
		config.setMinIdle(minIdle);
		return new JedisPool(config, host, port, connectionTimeout, socketTimeout, password, database, clientName);
	}

	public RedisClient lettuceClient() {
		return RedisClient.create(clientResources(), redisURI());
	}

	public RediSearchClient lettuSearchClient() {
		return RediSearchClient.create(clientResources(), redisURI());
	}

	private ClientResources clientResources() {
		Builder builder = DefaultClientResources.builder();
		if (showMetrics) {
			builder.commandLatencyCollectorOptions(DefaultCommandLatencyCollectorOptions.builder().enable().build());
			builder.commandLatencyPublisherOptions(
					DefaultEventPublisherOptions.builder().eventEmitInterval(Duration.ofSeconds(1)).build());
		}
		DefaultClientResources resources = builder.build();
		if (showMetrics) {
			resources.eventBus().get().filter(redisEvent -> redisEvent instanceof CommandLatencyEvent)
					.cast(CommandLatencyEvent.class).subscribe(e -> System.out.println(e.getLatencies()));
		}
		return resources;
	}

	private RedisURI redisURI() {
		RedisURI redisURI = RedisURI.create(host, port);
		if (password != null) {
			redisURI.setPassword(password);
		}
		redisURI.setTimeout(Duration.ofSeconds(commandTimeout));
		redisURI.setClientName(clientName);
		redisURI.setDatabase(database);
		return redisURI;
	}

	private <T extends StatefulRedisConnection<String, String>> GenericObjectPool<T> lettucePool(Supplier<T> supplier) {
		GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();
		GenericObjectPool<T> pool = ConnectionPoolSupport.createGenericObjectPool(supplier, config);
		pool.setMaxTotal(maxTotal);
		pool.setMaxIdle(maxIdle);
		pool.setMinIdle(minIdle);
		pool.setMaxWaitMillis(maxWait);
		return pool;
	}

	public GenericObjectPool<StatefulRedisConnection<String, String>> lettucePool() {
		RedisClient client = lettuceClient();
		return lettucePool(client::connect);
	}

	public GenericObjectPool<StatefulRediSearchConnection<String, String>> lettusearchPool() {
		RediSearchClient client = lettuSearchClient();
		return lettucePool(client::connect);
	}

	public boolean isJedis() {
		return driver == RedisDriver.jedis;
	}
}
