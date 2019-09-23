package com.redislabs.riot.cli.redis;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.riot.Riot;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.support.ConnectionPoolSupport;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.util.Pool;

public class RedisConnectionOptions {

	private final Logger log = LoggerFactory.getLogger(RedisConnectionOptions.class);

	@Option(names = "--driver", description = "Redis driver: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private RedisDriver driver = RedisDriver.lettuce;
	@Option(names = { "-s",
			"--server" }, description = "Redis server address (default: ${DEFAULT-VALUE})", paramLabel = "<host:port>")
	private List<Endpoint> servers = Arrays.asList(new Endpoint("localhost:6379"));
	@Option(names = "--sentinel", description = "Sentinel master", paramLabel = "<master>")
	private String sentinelMaster;
	@Option(names = "--auth", arity = "0..1", interactive = true, description = "Database login password", paramLabel = "<pwd>")
	private String password;
	@Option(names = "--db", description = "Redis database number", paramLabel = "<int>")
	private int database = 0;
	@Option(names = "--client", description = "Redis client name (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private String clientName = ClassUtils.getShortName(Riot.class).toLowerCase();
	@Option(names = "--ssl", description = "SSL connection")
	private boolean ssl;
	@Option(names = "--cluster", description = "Connect to a Redis cluster")
	private boolean cluster;
	@Option(names = "--max-redirects", description = "Max cluster redirects (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int maxRedirects = ClusterClientOptions.DEFAULT_MAX_REDIRECTS;
	@Option(names = "--max-total", description = "Max connections; -1 for no limit (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
	@Option(names = "--min-idle", description = "Min idle connections (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
	@Option(names = "--max-idle", description = "Max idle connections; -1 for no limit (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
	@Option(names = "--max-wait", description = "Max duration; -1 for infinite (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private long maxWait = GenericObjectPoolConfig.DEFAULT_MAX_WAIT_MILLIS;
	@ArgGroup(exclusive = false, heading = "Jedis connection options%n", order = 2)
	private JedisConnectionOptions jedis = new JedisConnectionOptions();
	@ArgGroup(exclusive = false, heading = "Lettuce connection options%n", order = 3)
	private LettuceConnectionOptions lettuce = new LettuceConnectionOptions();

	public boolean isCluster() {
		return cluster;
	}

	public JedisCluster jedisCluster() {
		Set<HostAndPort> hostAndPort = new HashSet<>();
		servers.forEach(node -> hostAndPort.add(new HostAndPort(node.getHost(), node.getPort())));
		if (password == null) {
			return new JedisCluster(hostAndPort, jedis.getConnectTimeout(), jedis.getSocketTimeout(), maxRedirects,
					configure(new JedisPoolConfig()));
		}
		return new JedisCluster(hostAndPort, jedis.getConnectTimeout(), jedis.getSocketTimeout(), maxRedirects,
				password, configure(new JedisPoolConfig()));
	}

	public Pool<Jedis> jedisPool() {
		JedisPoolConfig poolConfig = configure(new JedisPoolConfig());
		if (sentinelMaster == null) {
			String host = servers.get(0).getHost();
			int port = servers.get(0).getPort();
			log.debug("Creating Jedis connection pool for {}:{} with {}", host, port, poolConfig);
			return new JedisPool(poolConfig, host, port, jedis.getConnectTimeout(), jedis.getSocketTimeout(), password,
					database, clientName);
		}
		return new JedisSentinelPool(sentinelMaster,
				servers.stream().map(e -> e.toString()).collect(Collectors.toSet()), poolConfig,
				jedis.getConnectTimeout(), jedis.getSocketTimeout(), password, database, clientName);

	}

	@SuppressWarnings("rawtypes")
	private <T extends GenericObjectPoolConfig> T configure(T poolConfig) {
		poolConfig.setMaxTotal(maxTotal);
		poolConfig.setMaxIdle(maxIdle);
		poolConfig.setMinIdle(minIdle);
		poolConfig.setMaxWaitMillis(maxWait);
		poolConfig.setJmxEnabled(false);
		return poolConfig;
	}

	public <T extends StatefulConnection<String, String>> GenericObjectPool<T> pool(Supplier<T> supplier) {
		return ConnectionPoolSupport.createGenericObjectPool(supplier, configure(new GenericObjectPoolConfig<>()),
				false);
	}

	public RedisClient lettuceClient() {
		log.debug("Creating Lettuce client");
		RedisClient client = RedisClient.create(lettuce.clientResources(), redisURI());
		client.setOptions(lettuce.clientOptions(ssl, ClientOptions.builder()));
		return client;
	}

	public RedisClusterClient lettuceClusterClient() {
		log.debug("Creating Lettuce cluster client");
		RedisClusterClient client = RedisClusterClient.create(lettuce.clientResources(),
				servers.stream().map(e -> redisURI(e)).collect(Collectors.toList()));
		ClusterClientOptions.Builder builder = ClusterClientOptions.builder();
		builder.maxRedirects(maxRedirects);
		client.setOptions((ClusterClientOptions) lettuce.clientOptions(ssl, builder));
		return client;
	}

	public RediSearchClient lettuSearchClient() {
		log.debug("Creating LettuSearch client");
		RediSearchClient client = RediSearchClient.create(lettuce.clientResources(), redisURI());
		client.setOptions(lettuce.clientOptions(ssl, ClientOptions.builder()));
		return client;
	}

	private RedisURI redisURI() {
		if (sentinelMaster == null) {
			return redisURI(servers.get(0));
		}
		RedisURI.Builder builder = RedisURI.Builder.sentinel(servers.get(0).getHost(), servers.get(0).getPort(),
				sentinelMaster);
		servers.forEach(e -> builder.withSentinel(e.getHost(), e.getPort()));
		return redisURI(builder);
	}

	private RedisURI redisURI(Endpoint endpoint) {
		RedisURI.Builder builder = RedisURI.Builder.redis(endpoint.getHost()).withPort(endpoint.getPort());
		return redisURI(builder);
	}

	private RedisURI redisURI(RedisURI.Builder builder) {
		builder.withClientName(clientName).withDatabase(database).withTimeout(Duration.ofSeconds(getCommandTimeout()));
		if (password != null) {
			builder.withPassword(password);
		}
		if (ssl) {
			builder.withSsl(ssl);
		}
		return builder.build();
	}

	public boolean isJedis() {
		return driver == RedisDriver.jedis;
	}

	public long getCommandTimeout() {
		return lettuce.getCommandTimeout();
	}
}
