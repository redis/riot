package com.redis.riot.cli.common;

import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.writer.MergePolicy;
import com.redis.spring.batch.writer.ReplicaWaitWriteOperation;
import com.redis.spring.batch.writer.StreamIdPolicy;

import picocli.CommandLine.Option;

public class RedisWriterOptions {

	@Option(names = "--dry-run", description = "Enable dummy writes.")
	private boolean dryRun;

	@Option(names = "--multi-exec", description = "Enable MULTI/EXEC writes.")
	private boolean multiExec;

	@Option(names = "--wait-replicas", description = "Number of replicas for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int waitReplicas;

	@Option(names = "--wait-timeout", description = "Timeout in millis for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long waitTimeout = ReplicaWaitWriteOperation.DEFAULT_TIMEOUT.toMillis();

	@Option(names = "--write-pool", description = "Max connections for writer pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolMaxTotal = PoolOptions.DEFAULT_MAX_TOTAL;

	@Option(names = "--merge-policy", description = "Policy to merge collection data structures: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<p>")
	private MergePolicy mergePolicy = MergePolicy.OVERWRITE;

	@Option(names = "--stream-id", description = "Policy for stream IDs: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<p>")
	private StreamIdPolicy streamIdPolicy = StreamIdPolicy.PROPAGATE;

	public int getPoolMaxTotal() {
		return poolMaxTotal;
	}

	public void setPoolMaxTotal(int poolMaxTotal) {
		this.poolMaxTotal = poolMaxTotal;
	}

	public MergePolicy getMergePolicy() {
		return mergePolicy;
	}

	public void setMergePolicy(MergePolicy mergePolicy) {
		this.mergePolicy = mergePolicy;
	}

	public StreamIdPolicy getStreamIdPolicy() {
		return streamIdPolicy;
	}

	public void setStreamIdPolicy(StreamIdPolicy streamIdPolicy) {
		this.streamIdPolicy = streamIdPolicy;
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

}
