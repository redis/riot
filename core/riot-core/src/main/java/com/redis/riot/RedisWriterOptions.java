package com.redis.riot;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.spring.batch.RedisItemWriter.BaseRedisItemWriterBuilder;

import io.lettuce.core.api.StatefulConnection;
import picocli.CommandLine.Option;

public class RedisWriterOptions {

	@Option(names = "--multi-exec", description = "Enable MULTI/EXEC writes.")
	private boolean multiExec;
	@Option(names = "--wait-replicas", description = "Number of replicas for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int waitReplicas = 0;
	@Option(names = "--wait-timeout", description = "Timeout in millis for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long waitTimeout = 300;
	@Option(names = "--writer-pool", description = "Max pool connections (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolMaxTotal = 8;

	public boolean isMultiExec() {
		return multiExec;
	}

	public void setMultiExec(boolean multiExec) {
		this.multiExec = multiExec;
	}

	public int getWaitReplicas() {
		return waitReplicas;
	}

	public void setWaitReplicas(int waitReplicas) {
		this.waitReplicas = waitReplicas;
	}

	public long getWaitTimeout() {
		return waitTimeout;
	}

	public void setWaitTimeout(long waitTimeout) {
		this.waitTimeout = waitTimeout;
	}

	public int getPoolMaxTotal() {
		return poolMaxTotal;
	}

	public void setPoolMaxTotal(int poolMaxTotal) {
		this.poolMaxTotal = poolMaxTotal;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <B extends BaseRedisItemWriterBuilder> B configureWriter(B writer) {
		if (waitReplicas > 0) {
			writer.waitForReplication(waitReplicas, waitTimeout);
		}
		if (multiExec) {
			writer.multiExec();
		}
		GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(poolMaxTotal);
		writer.poolConfig(poolConfig);
		return writer;
	}
}
