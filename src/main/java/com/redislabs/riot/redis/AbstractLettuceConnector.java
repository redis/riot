package com.redislabs.riot.redis;

import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;

public abstract class AbstractLettuceConnector {

	private final Logger log = LoggerFactory.getLogger(AbstractLettuceConnector.class);

	private ClientResources resources;
	private RedisURI redisURI;
	private GenericObjectPoolConfig<? extends StatefulRedisConnection<String, String>> poolConfig;
	private AbstractRedisClient client;
	private GenericObjectPool<StatefulRedisConnection<String, String>> pool;

	protected AbstractLettuceConnector(ClientResources resources, RedisURI redisURI,
			GenericObjectPoolConfig<? extends StatefulRedisConnection<String, String>> poolConfig) {
		this.resources = resources;
		this.redisURI = redisURI;
		this.poolConfig = poolConfig;
	}

	public void open() {
		this.client = createClient(resources, redisURI);
		log.info("Creating Lettuce pool: {}", poolConfig);
		this.pool = ConnectionPoolSupport.createGenericObjectPool(supplier(client), poolConfig);
	}

	protected abstract Supplier<StatefulRedisConnection<String, String>> supplier(AbstractRedisClient redisClient);

	protected abstract AbstractRedisClient createClient(ClientResources resources, RedisURI redisURI);

	public boolean isClosed() {
		return pool.isClosed();
	}

	public StatefulRedisConnection<String, String> borrowConnection() throws Exception {
		return pool.borrowObject();
	}

	public void close() {
		// Take care of multi-threaded writer by only closing on the last call
		if (pool.getBorrowedCount() == 0) {
			pool.close();
			client.shutdown();
			resources.shutdown();
		}
	}

	public void returnConnection(StatefulRedisConnection<String, String> connection) {
		pool.returnObject(connection);
	}

}
