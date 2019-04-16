package com.redislabs.riot.cli;

import java.net.InetAddress;
import java.time.Duration;

import org.springframework.boot.autoconfigure.data.redis.RedisProperties.Pool;

import io.lettuce.core.RedisURI;
import lombok.Data;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(sortOptions = false)
@Data
public class RedisConnectionOptions {

	public static final String DEFAULT_HOST = "localhost";

	@Option(names = { "-h", "--host" }, description = "Redis server host (default: localhost).", order = 1)
	private InetAddress host;
	@Option(names = { "-p", "--port" }, description = "Redis server port (default: ${DEFAULT-VALUE}).", order = 2)
	private int port = RedisURI.DEFAULT_REDIS_PORT;
	@Option(names = "--timeout", description = "Redis command timeout in seconds for synchronous command execution (default: ${DEFAULT-VALUE}).", order = 3)
	private long timeout = RedisURI.DEFAULT_TIMEOUT;
	@Option(names = "--password", description = "Redis database password.", interactive = true, order = 3)
	private String password;
	@Option(names = "--max-idle", description = "Maximum number of idle connections in the pool (default: ${DEFAULT-VALUE}). Use a negative value to indicate an unlimited number of idle connections.", order = 3)
	private int maxIdle = 8;
	@Option(names = "--min-idle", description = "Target for the minimum number of idle connections to maintain in the pool (default: ${DEFAULT-VALUE}). This setting only has an effect if it is positive.", order = 3)
	private int minIdle = 0;
	@Option(names = "--max-active", description = "Maximum number of connections that can be allocated by the pool at a given time (default: ${DEFAULT-VALUE}). Use a negative value for no limit.", order = 3)
	private int maxActive = 8;
	@Option(names = "--max-wait", description = "Maximum amount of time in milliseconds a connection allocation should block before throwing an exception when the pool is exhausted. Use a negative value to block indefinitely (default).", order = 3)
	private Long maxWait = -1L;

	public Pool getPool() {
		Pool pool = new Pool();
		pool.setMaxActive(maxActive);
		pool.setMaxIdle(maxIdle);
		pool.setMaxWait(Duration.ofMillis(maxWait));
		pool.setMinIdle(minIdle);
		return pool;
	}

}