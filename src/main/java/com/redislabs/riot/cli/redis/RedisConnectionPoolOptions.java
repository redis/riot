package com.redislabs.riot.cli.redis;

import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import picocli.CommandLine.Option;

public class RedisConnectionPoolOptions {

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