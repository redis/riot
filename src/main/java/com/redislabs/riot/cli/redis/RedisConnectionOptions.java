package com.redislabs.riot.cli.redis;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.riot.redis.writer.JedisClusterItemWriter;
import com.redislabs.riot.redis.writer.JedisItemWriter;
import com.redislabs.riot.redis.writer.LettuceItemWriter;
import com.redislabs.riot.redis.writer.map.RedisMapWriter;
import com.redislabs.riot.redisearch.AbstractLettuSearchMapWriter;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;

public class RedisConnectionOptions {

	@Option(names = { "-s",
			"--server" }, description = "Redis server address (default: ${DEFAULT-VALUE})", paramLabel = "<host:port>")
	private List<RedisEndpoint> servers = Arrays.asList(new RedisEndpoint("localhost:6379"));
	@Option(names = "--sentinel-master", description = "Sentinel master name")
	private String sentinelMaster;
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
	@ArgGroup(exclusive = false, heading = "Redis connection pool options%n")
	private RedisConnectionPoolOptions poolOptions = new RedisConnectionPoolOptions();
	@ArgGroup(exclusive = false, heading = "Lettuce options%n")
	private LettuceConnectionOptions lettuce = new LettuceConnectionOptions();
	@ArgGroup(exclusive = false, heading = "Jedis options%n")
	private JedisConnectionOptions jedis = new JedisConnectionOptions();

	public List<RedisEndpoint> getServers() {
		return servers;
	}

	public String getSentinelMaster() {
		return sentinelMaster;
	}

	public boolean isSsl() {
		return ssl;
	}

	public int getMaxRedirects() {
		return maxRedirects;
	}

	public String getClientName() {
		return clientName;
	}

	public String getPassword() {
		return password;
	}

	public int getDatabase() {
		return database;
	}

	public Object redis() {
		if (driver == RedisDriver.jedis) {
			return jedis.pool(this).getResource();
		}
		if (cluster) {
			return lettuce.redisClusterClient(this).connect().sync();
		}
		return lettuce.redisClient(this).connect().sync();
	}

	public RedisConnectionPoolOptions getPoolOptions() {
		return poolOptions;
	}

	public RediSearchClient rediSearchClient() {
		return lettuce.rediSearchClient(this);
	}

	public ItemWriter<Map<String, Object>> writer(RedisMapWriter itemWriter) {
		if (itemWriter instanceof AbstractLettuSearchMapWriter) {
			RediSearchClient client = lettuce.rediSearchClient(this);
			return new LettuceItemWriter<StatefulRediSearchConnection<String, String>, RediSearchAsyncCommands<String, String>>(
					client, client::getResources, poolOptions.pool(client::connect), itemWriter,
					StatefulRediSearchConnection::async, lettuce.getCommandTimeout());
		}
		if (driver == RedisDriver.jedis) {
			if (cluster) {
				return new JedisClusterItemWriter(jedis.cluster(this), itemWriter);
			}
			return new JedisItemWriter(jedis.pool(this), itemWriter);
		}
		if (cluster) {
			RedisClusterClient client = lettuce.redisClusterClient(this);
			return new LettuceItemWriter<StatefulRedisClusterConnection<String, String>, RedisClusterAsyncCommands<String, String>>(
					client, client::getResources, poolOptions.pool(client::connect), itemWriter,
					StatefulRedisClusterConnection::async, lettuce.getCommandTimeout());
		}
		RedisClient client = lettuce.redisClient(this);
		return new LettuceItemWriter<StatefulRedisConnection<String, String>, RedisAsyncCommands<String, String>>(
				client, client::getResources, poolOptions.pool(client::connect), itemWriter,
				StatefulRedisConnection::async, lettuce.getCommandTimeout());
	}

	public JedisConnectionOptions jedis() {
		return jedis;
	}

	public Pool<Jedis> jedisPool() {
		return jedis.pool(this);
	}

}
