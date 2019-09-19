package com.redislabs.riot.cli.redis;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.riot.redis.writer.JedisClusterItemWriter;
import com.redislabs.riot.redis.writer.JedisItemWriter;
import com.redislabs.riot.redis.writer.LettuceItemWriter;
import com.redislabs.riot.redis.writer.map.RedisMapWriter;
import com.redislabs.riot.redisearch.AbstractLettuSearchMapWriter;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.DefaultClientResources.Builder;
import io.lettuce.core.support.ConnectionPoolSupport;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.Pool;

public class RedisConnectionOptions {

	private final Logger log = LoggerFactory.getLogger(RedisConnectionOptions.class);

	@Option(names = { "-s",
			"--server" }, description = "Redis server address (default: ${DEFAULT-VALUE})", paramLabel = "<host:port>")
	private List<RedisEndpoint> servers = Arrays.asList(new RedisEndpoint("localhost:6379"));
	@Option(names = "--sentinel-master", description = "Sentinel master name")
	private String sentinelMaster;
	@Option(names = "--connect-timeout", description = "Connect timeout (default: ${DEFAULT-VALUE})", paramLabel = "<millis>")
	private int connectTimeout = Protocol.DEFAULT_TIMEOUT;
	@Option(names = "--socket-timeout", description = "Socket timeout (default: ${DEFAULT-VALUE})", paramLabel = "<millis>")
	private int socketTimeout = Protocol.DEFAULT_TIMEOUT;
	@Option(names = "--auth", arity = "0..1", interactive = true, description = "Database login password", paramLabel = "<pwd>")
	private String password;
	@Option(names = "--db", description = "Redis database number. Databases are only available for Redis Standalone and Redis Master/Slave", paramLabel = "<int>")
	private int database = 0;
	@Option(names = "--client-name", description = "Redis client name (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String clientName = "riot";
	@Option(names = "--driver", description = "Redis driver: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private RedisDriver driver = RedisDriver.lettuce;
	@Option(names = "--ssl", description = "SSL connection")
	private boolean ssl;
	@Option(names = "--cluster", description = "Connect to a Redis cluster")
	private boolean cluster;
	@Option(names = "--cluster-max-redirects", description = "Number of maximal cluster redirects (-MOVED and -ASK) to follow in case a key was moved from one node to another node (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int maxRedirects = ClusterClientOptions.DEFAULT_MAX_REDIRECTS;
	@Option(names = "--lettuce-command-timeout", description = "Lettuce command timeout for synchronous command execution (default: ${DEFAULT-VALUE})", paramLabel = "<seconds>")
	private long commandTimeout = RedisURI.DEFAULT_TIMEOUT;
	@Option(names = "--lettuce-metrics", description = "Show Lettuce metrics")
	private boolean showMetrics;
	@Option(names = "--lettuce-publish-on-scheduler", description = "Enable Lettuce publish on scheduler (default: ${DEFAULT-VALUE})", negatable = true)
	private boolean publishOnScheduler = ClientOptions.DEFAULT_PUBLISH_ON_SCHEDULER;
	@Option(names = "--lettuce-auto-reconnect", description = "Enable Lettuce auto-reconnect (default: ${DEFAULT-VALUE})", negatable = true)
	private boolean autoReconnect = ClientOptions.DEFAULT_AUTO_RECONNECT;
	@Option(names = "--lettuce-request-queue-size", description = "Per-connection request queue size (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int requestQueueSize = ClientOptions.DEFAULT_REQUEST_QUEUE_SIZE;
	@Option(names = "--lettuce-ssl-provider", description = "SSL Provider: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private SslProvider sslProvider = SslProvider.Jdk;
	@Option(names = "--keystore", description = "Path to keystore", paramLabel = "<file>")
	private File keystore;
	@Option(names = "--keystore-password", arity = "0..1", interactive = true, description = "Keystore password", paramLabel = "<pwd>")
	private String keystorePassword;
	@Option(names = "--truststore", description = "Path to truststore", paramLabel = "<file>")
	private File truststore;
	@Option(names = "--truststore-password", arity = "0..1", interactive = true, description = "Truststore password", paramLabel = "<pwd>")
	private String truststorePassword;
	@ArgGroup(exclusive = false, heading = "Redis connection pool options%n")
	private RedisConnectionPoolOptions poolOptions = new RedisConnectionPoolOptions();

	static class RedisConnectionPoolOptions {
		@Option(names = "--pool-max-total", description = "Max connections that can be allocated by the pool at a given time. Use negative value for no limit (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
		private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
		@Option(names = "--pool-min-idle", description = "Min idle connections in pool. Only has an effect if >0 (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
		private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
		@Option(names = "--pool-max-idle", description = "Max idle connections in pool. Use negative value for no limit (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
		private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
		@Option(names = "--pool-max-wait", description = "Max duration a connection allocation should block before throwing an exception when pool is exhausted. Use negative value to block indefinitely (default: ${DEFAULT-VALUE})", paramLabel = "<millis>")
		private long maxWait = GenericObjectPoolConfig.DEFAULT_MAX_WAIT_MILLIS;

		@SuppressWarnings("rawtypes")
		public <T extends GenericObjectPoolConfig> T configure(T poolConfig) {
			poolConfig.setMaxTotal(maxTotal);
			poolConfig.setMaxIdle(maxIdle);
			poolConfig.setMinIdle(minIdle);
			poolConfig.setMaxWaitMillis(maxWait);
			poolConfig.setJmxEnabled(false);
			return poolConfig;
		}

		public <T extends StatefulConnection<String, String>> GenericObjectPool<T> pool(Supplier<T> supplier) {
			return ConnectionPoolSupport.createGenericObjectPool(supplier, configure(new GenericObjectPoolConfig<>()));
		}

	}

	private RedisURI uri() {
		if (sentinelMaster == null) {
			return uri(servers.get(0));
		}
		RedisURI.Builder builder = RedisURI.Builder.sentinel(servers.get(0).getHost(), servers.get(0).getPort(),
				sentinelMaster);
		servers.forEach(e -> builder.withSentinel(e.getHost(), e.getPort()));
		return uri(builder);
	}

	private RedisURI uri(RedisEndpoint endpoint) {
		return uri(RedisURI.Builder.redis(endpoint.getHost()).withPort(endpoint.getPort()));
	}

	private RedisURI uri(RedisURI.Builder builder) {
		builder.withClientName(clientName).withDatabase(database).withTimeout(Duration.ofSeconds(commandTimeout));
		if (password != null) {
			builder.withPassword(password);
		}
		if (ssl) {
			builder.withSsl(ssl);
		}
		return builder.build();
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

	public Pool<Jedis> jedisPool() {
		JedisPoolConfig poolConfig = poolOptions.configure(new JedisPoolConfig());
		if (sentinelMaster == null) {
			String host = servers.get(0).getHost();
			int port = servers.get(0).getPort();
			log.debug("Creating Jedis connection pool for {}:{} with {}", host, port, poolConfig);
			return new JedisPool(poolConfig, host, port, connectTimeout, socketTimeout, password, database, clientName);
		}
		return new JedisSentinelPool(sentinelMaster,
				servers.stream().map(e -> e.toString()).collect(Collectors.toSet()), poolConfig, connectTimeout,
				socketTimeout, password, database, clientName);
	}

	private RedisClient client() {
		log.debug("Creating Lettuce client");
		RedisClient client = RedisClient.create(resources(), uri());
		client.setOptions(clientOptions());
		return client;
	}

	private RedisClusterClient clusterClient() {
		log.debug("Creating Lettuce cluster client");
		RedisClusterClient client = RedisClusterClient.create(resources(),
				servers.stream().map(e -> uri(e)).collect(Collectors.toList()));
		ClusterClientOptions.Builder builder = ClusterClientOptions.builder();
		builder.maxRedirects(maxRedirects);
		client.setOptions((ClusterClientOptions) clientOptions(builder));
		return client;
	}

	public RediSearchClient rediSearchClient() {
		log.debug("Creating LettuSearch client");
		RediSearchClient client = RediSearchClient.create(resources(), uri());
		client.setOptions(clientOptions());
		return client;
	}

	private ClientOptions clientOptions() {
		return clientOptions(ClientOptions.builder());
	}

	private ClientOptions clientOptions(ClientOptions.Builder builder) {
		if (ssl) {
			builder.sslOptions(sslOptions());
		}
		builder.publishOnScheduler(publishOnScheduler);
		builder.autoReconnect(autoReconnect);
		builder.requestQueueSize(requestQueueSize);
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

	public Object redis() {
		if (driver == RedisDriver.jedis) {
			return jedisPool().getResource();
		}
		if (cluster) {
			return clusterClient().connect().sync();
		}
		return client().connect().sync();
	}

	public ItemWriter<Map<String, Object>> writer(RedisMapWriter itemWriter) {
		if (itemWriter instanceof AbstractLettuSearchMapWriter) {
			RediSearchClient client = rediSearchClient();
			return new LettuceItemWriter<StatefulRediSearchConnection<String, String>, RediSearchAsyncCommands<String, String>>(
					client, client::getResources, poolOptions.pool(client::connect), itemWriter,
					StatefulRediSearchConnection::async);
		}
		if (driver == RedisDriver.jedis) {
			if (cluster) {
				return new JedisClusterItemWriter(jedisCluster(), itemWriter);
			}
			return new JedisItemWriter(jedisPool(), itemWriter);
		}
		if (cluster) {
			RedisClusterClient client = clusterClient();
			return new LettuceItemWriter<StatefulRedisClusterConnection<String, String>, RedisClusterAsyncCommands<String, String>>(
					client, client::getResources, poolOptions.pool(client::connect), itemWriter,
					StatefulRedisClusterConnection::async);
		}
		RedisClient client = client();
		return new LettuceItemWriter<StatefulRedisConnection<String, String>, RedisAsyncCommands<String, String>>(
				client, client::getResources, poolOptions.pool(client::connect), itemWriter,
				StatefulRedisConnection::async);
	}

	private JedisCluster jedisCluster() {
		Set<HostAndPort> hostAndPort = new HashSet<>();
		servers.forEach(node -> hostAndPort.add(new HostAndPort(node.getHost(), node.getPort())));
		JedisPoolConfig poolConfig = poolOptions.configure(new JedisPoolConfig());
		if (password == null) {
			return new JedisCluster(hostAndPort, connectTimeout, socketTimeout, maxRedirects, poolConfig);
		}
		return new JedisCluster(hostAndPort, connectTimeout, socketTimeout, maxRedirects, password, poolConfig);
	}

}
