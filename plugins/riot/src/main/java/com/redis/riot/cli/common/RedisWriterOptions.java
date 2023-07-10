package com.redis.riot.cli.common;

import java.time.Duration;

import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.writer.ReplicaWaitOptions;
import com.redis.spring.batch.writer.WriterOptions;

import picocli.CommandLine.Option;

public class RedisWriterOptions {

	@Option(names = "--dry-run", description = "Enable dummy writes.")
	private boolean dryRun;

	@Option(names = "--multi-exec", description = "Enable MULTI/EXEC writes.")
	private boolean multiExec;

	@Option(names = "--wait-replicas", description = "Number of replicas for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int waitReplicas;

	@Option(names = "--wait-timeout", description = "Timeout in millis for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long waitTimeout = ReplicaWaitOptions.DEFAULT_TIMEOUT.toMillis();

	@Option(names = "--write-pool", description = "Max connections for writer pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolMaxTotal = PoolOptions.DEFAULT_MAX_TOTAL;

	public int getPoolMaxTotal() {
		return poolMaxTotal;
	}

	public void setPoolMaxTotal(int poolMaxTotal) {
		this.poolMaxTotal = poolMaxTotal;
	}

	public boolean isDryRun() {
		return dryRun;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}

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

	public WriterOptions writerOptions() {
		return WriterOptions.builder().multiExec(multiExec).poolOptions(poolOptions())
				.replicaWaitOptions(replicaWaitOptions()).build();
	}

	private ReplicaWaitOptions replicaWaitOptions() {
		return ReplicaWaitOptions.builder().replicas(waitReplicas).timeout(waitTimeoutDuration()).build();
	}

	private Duration waitTimeoutDuration() {
		return Duration.ofMillis(waitTimeout);
	}

	private PoolOptions poolOptions() {
		return PoolOptions.builder().maxTotal(poolMaxTotal).build();
	}

	@Override
	public String toString() {
		return "RedisWriterOptions [dryRun=" + dryRun + ", multiExec=" + multiExec + ", waitReplicas=" + waitReplicas
				+ ", waitTimeout=" + waitTimeout + ", poolMaxTotal=" + poolMaxTotal + "]";
	}

}
