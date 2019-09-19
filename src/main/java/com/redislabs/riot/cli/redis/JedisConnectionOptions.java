package com.redislabs.riot.cli.redis;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine.Option;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.Pool;

public class JedisConnectionOptions {

	private final Logger log = LoggerFactory.getLogger(JedisConnectionOptions.class);

	@Option(names = "--connect-timeout", description = "Connect timeout (default: ${DEFAULT-VALUE})", paramLabel = "<millis>")
	private int connectTimeout = Protocol.DEFAULT_TIMEOUT;
	@Option(names = "--socket-timeout", description = "Socket timeout (default: ${DEFAULT-VALUE})", paramLabel = "<millis>")
	private int socketTimeout = Protocol.DEFAULT_TIMEOUT;

	public Pool<Jedis> pool(RedisConnectionOptions options) {
		JedisPoolConfig poolConfig = options.getPoolOptions().configure(new JedisPoolConfig());
		if (options.getSentinelMaster() == null) {
			String host = options.getServers().get(0).getHost();
			int port = options.getServers().get(0).getPort();
			log.debug("Creating Jedis connection pool for {}:{} with {}", host, port, poolConfig);
			return new JedisPool(poolConfig, host, port, connectTimeout, socketTimeout, options.getPassword(),
					options.getDatabase(), options.getClientName());
		}
		return new JedisSentinelPool(options.getSentinelMaster(),
				options.getServers().stream().map(e -> e.toString()).collect(Collectors.toSet()), poolConfig,
				connectTimeout, socketTimeout, options.getPassword(), options.getDatabase(), options.getClientName());
	}

	public JedisCluster cluster(RedisConnectionOptions options) {
		Set<HostAndPort> hostAndPort = new HashSet<>();
		options.getServers().forEach(node -> hostAndPort.add(new HostAndPort(node.getHost(), node.getPort())));
		JedisPoolConfig poolConfig = options.getPoolOptions().configure(new JedisPoolConfig());
		if (options.getPassword() == null) {
			return new JedisCluster(hostAndPort, connectTimeout, socketTimeout, options.getMaxRedirects(), poolConfig);
		}
		return new JedisCluster(hostAndPort, connectTimeout, socketTimeout, options.getMaxRedirects(),
				options.getPassword(), poolConfig);
	}

}
