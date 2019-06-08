package com.redislabs.riot.cli;

import java.net.InetAddress;
import java.time.Duration;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.riot.cli.in.Import;
import com.redislabs.riot.cli.out.Export;

import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.Getter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

@Command(name = "riot", subcommands = { Export.class, Import.class })
public class RootCommand extends AbstractCommand {

	public static final String DEFAULT_HOST = "localhost";

	/**
	 * Just to avoid picocli complain in Eclipse console
	 */
	@Option(names = "--spring.output.ansi.enabled", hidden = true)
	private String ansiEnabled;
	@Getter
	@Option(names = "--host", description = "Redis server host. (default: localhost).")
	private InetAddress host;
	@Option(names = "--port", description = "Redis server port. (default: ${DEFAULT-VALUE}).")
	private int port = RedisURI.DEFAULT_REDIS_PORT;
	@Option(names = "--command-timeout", description = "Redis command timeout in seconds for synchronous command execution (default: ${DEFAULT-VALUE}).")
	private long commandTimeout = RedisURI.DEFAULT_TIMEOUT;
	@Option(names = "--connection-timeout", description = "Redis connect timeout in milliseconds. (default: ${DEFAULT-VALUE}).")
	private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
	@Option(names = "--socket-timeout", description = "Redis socket timeout in milliseconds. (default: ${DEFAULT-VALUE}).")
	private int socketTimeout = Protocol.DEFAULT_TIMEOUT;
	@Option(names = "--password", description = "Redis database password.", interactive = true)
	private String password;
	@Option(names = "--max-idle", description = "Maximum number of idle connections in the pool. Use a negative value to indicate an unlimited number of idle connections. (default: ${DEFAULT-VALUE}).")
	private int maxIdle = 8;
	@Option(names = "--min-idle", description = "Target for the minimum number of idle connections to maintain in the pool. This setting only has an effect if it is positive. (default: ${DEFAULT-VALUE}).")
	private int minIdle = 0;
	@Option(names = "--max-total", description = "Maximum number of connections that can be allocated by the pool at a given time. Use a negative value for no limit. (default: ${DEFAULT-VALUE})")
	private int maxTotal = 8;
	@Option(names = "--max-wait", description = "Maximum amount of time in milliseconds a connection allocation should block before throwing an exception when the pool is exhausted. Use a negative value to block indefinitely (default).")
	private long maxWait = -1L;
	@Option(names = "--database", description = "Redis database number. Databases are only available for Redis Standalone and Redis Master/Slave. (default: ${DEFAULT-VALUE}).")
	private int database = 0;
	@Option(names = "--client-name", description = "Redis client name.")
	private String clientName;

	public String getHostname() {
		if (host != null) {
			return host.getHostName();
		}
		return DEFAULT_HOST;
	}

	public JedisPool jedisPool() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(maxIdle);
		config.setMaxTotal(maxTotal);
		config.setMaxWaitMillis(maxWait);
		config.setMinIdle(minIdle);
		return new JedisPool(config, getHostname(), port, connectionTimeout, socketTimeout, password, database,
				clientName);
	}

	public RediSearchClient lettuceClient() {
		RedisURI redisURI = RedisURI.create(getHostname(), port);
		if (password != null) {
			redisURI.setPassword(password);
		}
		redisURI.setTimeout(Duration.ofSeconds(commandTimeout));
		redisURI.setClientName(clientName);
		redisURI.setDatabase(database);
		return RediSearchClient.create(DefaultClientResources.create(), redisURI);
	}

	public GenericObjectPool<StatefulRedisConnection<String, String>> lettucePool() {
		RediSearchClient client = lettuceClient();
		GenericObjectPoolConfig<StatefulRedisConnection<String, String>> config = new GenericObjectPoolConfig<StatefulRedisConnection<String, String>>();
		GenericObjectPool<StatefulRedisConnection<String, String>> pool = ConnectionPoolSupport
				.createGenericObjectPool(() -> client.connect(), config);
		pool.setMaxTotal(maxTotal);
		pool.setMaxIdle(maxIdle);
		pool.setMinIdle(minIdle);
		pool.setMaxWaitMillis(maxWait);
		return pool;
	}
}
