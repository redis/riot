package com.redislabs.riot.cli.redis;

import java.io.File;
import java.time.Duration;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redislabs.lettusearch.RediSearchClient;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.DefaultClientResources.Builder;
import picocli.CommandLine.Option;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class RedisConnectionOptions {

	private final Logger log = LoggerFactory.getLogger(RedisConnectionOptions.class);

	@Option(names = { "-s",
			"--server" }, description = "Redis server address (default: ${DEFAULT-VALUE})", paramLabel = "<host>")
	private String host = "localhost";
	@Option(names = { "-p",
			"--port" }, description = "Redis server port (default: ${DEFAULT-VALUE})", paramLabel = "<integer>")
	private int port = RedisURI.DEFAULT_REDIS_PORT;
	@Option(names = "--command-timeout", description = "Command timeout in seconds for synchronous command execution (default: ${DEFAULT-VALUE})", paramLabel = "<seconds>")
	private long commandTimeout = RedisURI.DEFAULT_TIMEOUT;
	@Option(names = "--connection-timeout", description = "Connect timeout in milliseconds (default: ${DEFAULT-VALUE})", paramLabel = "<millis>")
	private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
	@Option(names = "--socket-timeout", description = "Socket timeout in milliseconds (default: ${DEFAULT-VALUE})", paramLabel = "<millis>")
	private int socketTimeout = Protocol.DEFAULT_TIMEOUT;
	@Option(names = "--auth", arity = "0..1", interactive = true, description = "Database login password", paramLabel = "<pwd>")
	private String password;
	@Option(names = "--db", description = "Redis database number. Databases are only available for Redis Standalone and Redis Master/Slave", paramLabel = "<number>")
	private int database = 0;
	@Option(names = "--client-name", description = "Redis client name (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String clientName = "RIOT";
	@Option(names = "--metrics", description = "Show metrics (only works with Lettuce driver)")
	private boolean showMetrics;
	@Option(names = "--driver", description = "Redis driver: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private RedisDriver driver = RedisDriver.lettuce;
	@Option(names = "--ssl", description = "SSL connection")
	private boolean ssl;
	@Option(names = "--max-total", description = "Maximum number of connections that can be allocated by the pool at a given time. Use a negative value for no limit (default: ${DEFAULT-VALUE})", paramLabel = "<integer>")
	private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
	@Option(names = "--min-idle", description = "Target for the minimum number of idle connections to maintain in the pool. This setting only has an effect if it is positive (default: ${DEFAULT-VALUE})", paramLabel = "<integer>")
	private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
	@Option(names = "--max-idle", description = "Maximum number of idle connections in the pool. Use a negative value to indicate an unlimited number of idle connections (default: ${DEFAULT-VALUE})", paramLabel = "<integer>")
	private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
	@Option(names = "--max-wait", description = "Maximum amount of time a connection allocation should block before throwing an exception when the pool is exhausted. Use a negative value to block indefinitely (default: ${DEFAULT-VALUE})", paramLabel = "<millis>")
	private long maxWait = GenericObjectPoolConfig.DEFAULT_MAX_WAIT_MILLIS;
	@Option(names = "--ssl-provider", description = "SSL Provider: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private SslProvider sslProvider = SslProvider.Jdk;
	@Option(names = "--keystore", description = "Path to keystore", paramLabel = "<file>")
	private File keystore;
	@Option(names = "--keystore-password", arity = "0..1", interactive = true, description = "Keystore password", paramLabel = "<pwd>")
	private String keystorePassword;
	@Option(names = "--truststore", description = "Path to truststore", paramLabel = "<file>")
	private File truststore;
	@Option(names = "--truststore-password", arity = "0..1", interactive = true, description = "Truststore password", paramLabel = "<pwd>")
	private String truststorePassword;

	@SuppressWarnings("rawtypes")
	private <T extends GenericObjectPoolConfig> T configure(T poolConfig) {
		poolConfig.setMaxTotal(maxTotal);
		poolConfig.setMaxIdle(maxIdle);
		poolConfig.setMinIdle(minIdle);
		poolConfig.setMaxWaitMillis(maxWait);
		return poolConfig;
	}

	public RedisDriver getDriver() {
		return driver;
	}

	private RedisURI redisUri() {
		RedisURI redisURI = RedisURI.create(host, port);
		if (password != null) {
			redisURI.setPassword(password);
		}
		redisURI.setTimeout(Duration.ofSeconds(commandTimeout));
		redisURI.setClientName(clientName);
		redisURI.setDatabase(database);
		return redisURI;
	}

	private ClientResources resources() {
		Builder builder = DefaultClientResources.builder();
		if (showMetrics) {
			builder.commandLatencyCollectorOptions(DefaultCommandLatencyCollectorOptions.builder().enable().build());
			builder.commandLatencyPublisherOptions(
					DefaultEventPublisherOptions.builder().eventEmitInterval(Duration.ofSeconds(1)).build());
		}
		ClientResources resources = builder.build();
		if (showMetrics) {
			resources.eventBus().get().filter(redisEvent -> redisEvent instanceof CommandLatencyEvent)
					.cast(CommandLatencyEvent.class).subscribe(e -> log.info(String.valueOf(e.getLatencies())));
		}
		return resources;
	}

	public JedisPool jedisPool() {
		JedisPoolConfig poolConfig = configure(new JedisPoolConfig());
		log.debug("Creating Jedis connection pool for {}:{} with {}", host, port, poolConfig);
		return new JedisPool(poolConfig, host, port, connectionTimeout, socketTimeout, password, database, clientName);
	}

	public RedisClient redisClient() {
		log.debug("Creating Lettuce client");
		RedisClient client = RedisClient.create(resources(), redisUri());
		client.setOptions(clientOptions());
		return client;
	}

	public RediSearchClient rediSearchClient() {
		log.debug("Creating LettuSearch client");
		RediSearchClient client = RediSearchClient.create(resources(), redisUri());
		client.setOptions(clientOptions());
		return client;
	}

	private ClientOptions clientOptions() {
		io.lettuce.core.ClientOptions.Builder builder = ClientOptions.builder();
		if (ssl) {
			builder.sslOptions(sslOptions());
		}
		return builder.build();
	}

	private SslOptions sslOptions() {
		SslOptions.Builder builder = SslOptions.builder();
		switch (sslProvider) {
		case OpenSsl:
			builder.openSslProvider();
			break;
		default:
			builder.jdkSslProvider();
			break;
		}
		if (keystore != null) {
			if (keystorePassword == null) {
				builder.keystore(keystore);
			} else {
				builder.keystore(keystore, keystorePassword.toCharArray());
			}
		}
		if (truststore != null) {
			if (truststorePassword == null) {
				builder.truststore(truststore);
			} else {
				builder.truststore(truststore, truststorePassword);
			}
		}
		return builder.build();
	}

	public <T> GenericObjectPoolConfig<T> poolConfig() {
		return configure(new GenericObjectPoolConfig<>());
	}
}
