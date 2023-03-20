package com.redis.riot;

import java.time.Duration;
import java.util.Optional;

import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.writer.WaitForReplication;
import com.redis.spring.batch.writer.WriterOptions;
import com.redis.spring.batch.writer.WriterOptions.Builder;

import picocli.CommandLine.Option;

public class RedisWriterOptions {

	@Option(names = "--dry-run", description = "Enable dummy writes.")
	private boolean dryRun;
	@Option(names = "--multi-exec", description = "Enable MULTI/EXEC writes.")
	private boolean multiExec;
	@Option(names = "--wait-replicas", description = "Number of replicas for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int waitReplicas;
	@Option(names = "--wait-timeout", description = "Timeout in millis for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long waitTimeout = WaitForReplication.DEFAULT_TIMEOUT.toMillis();
	@Option(names = "--write-pool", description = "Max connections for writer pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolMaxTotal = PoolOptions.DEFAULT_MAX_TOTAL;

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
		Builder builder = WriterOptions.builder();
		builder.waitForReplication(waitForReplication());
		builder.multiExec(multiExec);
		builder.poolOptions(poolOptions());
		return builder.build();
	}

	private Optional<WaitForReplication> waitForReplication() {
		if (waitReplicas > 0) {
			return Optional.of(WaitForReplication.of(waitReplicas, Duration.ofMillis(waitTimeout)));
		}
		return Optional.empty();
	}

	private PoolOptions poolOptions() {
		return PoolOptions.builder().maxTotal(poolMaxTotal).build();
	}

}
