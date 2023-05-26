package com.redis.riot.cli.common;

import java.time.Duration;
import java.util.Optional;

import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.writer.DataStructureWriteOptions;
import com.redis.spring.batch.writer.DataStructureWriteOptions.MergePolicy;
import com.redis.spring.batch.writer.DataStructureWriteOptions.StreamIdPolicy;
import com.redis.spring.batch.writer.ReplicaOptions;
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
	private long waitTimeout = ReplicaOptions.DEFAULT_TIMEOUT.toMillis();

	@Option(names = "--write-pool", description = "Max connections for writer pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolMaxTotal = PoolOptions.DEFAULT_MAX_TOTAL;

	@Option(names = "--merge-policy", description = "Policy to merge collection data structures: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<p>")
	private MergePolicy mergePolicy = MergePolicy.OVERWRITE;

	@Option(names = "--stream-id", description = "Policy for stream IDs: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<p>")
	private StreamIdPolicy streamIdPolicy = StreamIdPolicy.PROPAGATE;

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
		return WriterOptions.builder().replicaOptions(replicaOptions()).multiExec(multiExec).poolOptions(poolOptions())
				.build();
	}

	public DataStructureWriteOptions dataStructureOptions() {
		return DataStructureWriteOptions.builder().mergePolicy(mergePolicy).streamIdPolicy(streamIdPolicy).build();
	}

	private Optional<ReplicaOptions> replicaOptions() {
		if (waitReplicas > 0) {
			return Optional.of(
					ReplicaOptions.builder().replicas(waitReplicas).timeout(Duration.ofMillis(waitTimeout)).build());
		}
		return Optional.empty();
	}

	private PoolOptions poolOptions() {
		return PoolOptions.builder().maxTotal(poolMaxTotal).build();
	}

}
