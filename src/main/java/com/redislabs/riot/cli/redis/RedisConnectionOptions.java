package com.redislabs.riot.cli.redis;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.RedisURI;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.DefaultClientResources.Builder;
import picocli.CommandLine.Option;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;

public class RedisConnectionOptions {

	private final Logger log = LoggerFactory.getLogger(RedisConnectionOptions.class);

	@Option(names = { "-h", "--host" }, description = "Redis host (default: ${DEFAULT-VALUE})")
	protected String host = "localhost";
	@Option(names = { "-p",
			"--port" }, description = "Redis port (default: ${DEFAULT-VALUE})", paramLabel = "<integer>")
	protected int port = RedisURI.DEFAULT_REDIS_PORT;
	@Option(names = "--command-timeout", description = "Command timeout in seconds for synchronous command execution (default: ${DEFAULT-VALUE})", paramLabel = "<seconds>")
	protected long commandTimeout = RedisURI.DEFAULT_TIMEOUT;
	@Option(names = "--connection-timeout", description = "Connect timeout in milliseconds (default: ${DEFAULT-VALUE})", paramLabel = "<millis>")
	protected int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
	@Option(names = "--socket-timeout", description = "Socket timeout in milliseconds (default: ${DEFAULT-VALUE})", paramLabel = "<millis>")
	protected int socketTimeout = Protocol.DEFAULT_TIMEOUT;
	@Option(names = "--auth", arity = "0..1", interactive = true, description = "Database login password", paramLabel = "<string>")
	protected String password;
	@Option(names = "--db", description = "Redis database number. Databases are only available for Redis Standalone and Redis Master/Slave")
	protected int database = 0;
	@Option(names = "--client-name", description = "Redis client name (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	protected String clientName = "RIOT";
	@Option(names = "--metrics", description = "Show metrics (only works with Lettuce driver)")
	protected boolean showMetrics;
	@Option(names = "--client", description = "Redis driver: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})")
	protected RedisClient client = RedisClient.lettuce;

	public RedisURI redisUri() {
		RedisURI redisURI = RedisURI.create(host, port);
		if (password != null) {
			redisURI.setPassword(password);
		}
		redisURI.setTimeout(Duration.ofSeconds(commandTimeout));
		redisURI.setClientName(clientName);
		redisURI.setDatabase(database);
		return redisURI;
	}

	public ClientResources clientResources() {
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

	public Jedis jedis() {
		log.info("Creating Jedis connection");
		Jedis jedis = new Jedis(host, port, connectionTimeout, socketTimeout);
		try {
			jedis.connect();
			if (password != null) {
				jedis.auth(password);
			}
			if (database != 0) {
				jedis.select(database);
			}
			if (clientName != null) {
				jedis.clientSetname(clientName);
			}
			return jedis;
		} catch (JedisException je) {
			jedis.close();
			throw je;
		}
	}

}
