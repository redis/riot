package com.redis.riot.core;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.resource.ClientResources;

public class RedisClientOptions {

	public static final String DEFAULT_REDIS_HOST = "127.0.0.1";
	public static final int DEFAULT_REDIS_PORT = RedisURI.DEFAULT_REDIS_PORT;
	public static final RedisURI DEFAULT_REDIS_URI = RedisURI.create(DEFAULT_REDIS_HOST, DEFAULT_REDIS_PORT);

	private RedisURI redisURI = DEFAULT_REDIS_URI;
	private boolean cluster;
	private ClientOptions options;
	private ClientResources resources;

	public AbstractRedisClient redisClient() {
		if (cluster) {
			RedisModulesClusterClient client = clusterClient();
			if (options != null) {
				client.setOptions((ClusterClientOptions) options);
			}
			return client;
		}
		RedisModulesClient client = client();
		if (options != null) {
			client.setOptions(options);
		}
		return client;
	}

	private RedisModulesClient client() {
		if (resources == null) {
			return RedisModulesClient.create(redisURI);
		}
		return RedisModulesClient.create(resources, redisURI);
	}

	private RedisModulesClusterClient clusterClient() {
		if (resources == null) {
			return RedisModulesClusterClient.create(redisURI);
		}
		return RedisModulesClusterClient.create(resources, redisURI);
	}

	public RedisURI getRedisURI() {
		return redisURI;
	}

	public void setRedisURI(RedisURI uri) {
		this.redisURI = uri;
	}

	public boolean isCluster() {
		return cluster;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	public ClientOptions getOptions() {
		return options;
	}

	public void setOptions(ClientOptions options) {
		this.options = options;
	}

	public ClientResources getResources() {
		return resources;
	}

	public void setResources(ClientResources resources) {
		this.resources = resources;
	}

}
