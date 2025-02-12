package com.redis.riot;

import java.time.temporal.ChronoUnit;

import com.redis.riot.core.RiotDuration;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.writer.KeyValueWrite;
import com.redis.spring.batch.item.redis.writer.KeyValueWrite.WriteMode;

import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class RedisWriterArgs {

	public static final RiotDuration DEFAULT_WAIT_TIMEOUT = RiotDuration.of(RedisItemWriter.DEFAULT_WAIT_TIMEOUT,
			ChronoUnit.SECONDS);
	public static final int DEFAULT_POOL_SIZE = RedisItemWriter.DEFAULT_POOL_SIZE;

	@Option(names = "--multi-exec", description = "Enable MULTI/EXEC writes.")
	private boolean multiExec;

	@Option(names = "--wait-replicas", description = "Number of replicas for WAIT command (default: 0 i.e. no wait).", paramLabel = "<int>")
	private int waitReplicas;

	@Option(names = "--wait-timeout", description = "Timeout for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
	private RiotDuration waitTimeout = DEFAULT_WAIT_TIMEOUT;

	@Option(names = "--merge", description = "Merge collection data structures (hash, list, ...) instead of overwriting them. Only used in `--struct` mode.")
	private boolean merge;

	public <K, V, T> void configure(RedisItemWriter<K, V, T> writer) {
		writer.setMultiExec(multiExec);
		writer.setWaitReplicas(waitReplicas);
		writer.setWaitTimeout(waitTimeout.getValue());
		if (writer.getOperation() instanceof KeyValueWrite) {
			((KeyValueWrite<?, ?>) writer.getOperation()).setMode(merge ? WriteMode.MERGE : WriteMode.OVERWRITE);
		}
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

	public RiotDuration getWaitTimeout() {
		return waitTimeout;
	}

	public void setWaitTimeout(RiotDuration waitTimeout) {
		this.waitTimeout = waitTimeout;
	}

	public boolean isMerge() {
		return merge;
	}

	public void setMerge(boolean merge) {
		this.merge = merge;
	}

}
