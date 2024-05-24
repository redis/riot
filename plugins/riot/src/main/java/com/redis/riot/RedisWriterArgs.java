package com.redis.riot;

import java.time.Duration;

import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.writer.KeyValueWrite;
import com.redis.spring.batch.item.redis.writer.KeyValueWrite.WriteMode;

import picocli.CommandLine.Option;

public class RedisWriterArgs {

	public static final Duration DEFAULT_WAIT_TIMEOUT = RedisItemWriter.DEFAULT_WAIT_TIMEOUT;

	@Option(names = "--multi-exec", description = "Enable MULTI/EXEC writes.")
	private boolean multiExec;

	@Option(names = "--wait-replicas", description = "Number of replicas for WAIT command (default: 0 i.e. no wait).", paramLabel = "<int>")
	private int waitReplicas;

	@Option(names = "--wait-timeout", description = "Timeout in millis for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private Duration waitTimeout = DEFAULT_WAIT_TIMEOUT;

	@Option(names = "--merge", description = "Merge properties from collection data structures (`hash`, `set`, ...) instead of overwriting them.")
	private boolean merge;

	public void configure(RedisItemWriter<?, ?, ?> writer) {
		writer.setMultiExec(multiExec);
		writer.setWaitReplicas(waitReplicas);
		writer.setWaitTimeout(waitTimeout);
		if (writer.getOperation() instanceof KeyValueWrite) {
			((KeyValueWrite<?, ?>) writer.getOperation()).setMode(writeMode());
		}
	}

	private WriteMode writeMode() {
		if (merge) {
			return WriteMode.MERGE;
		}
		return WriteMode.OVERWRITE;
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

	public Duration getWaitTimeout() {
		return waitTimeout;
	}

	public void setWaitTimeout(Duration waitTimeout) {
		this.waitTimeout = waitTimeout;
	}

	public boolean isMerge() {
		return merge;
	}

	public void setMerge(boolean merge) {
		this.merge = merge;
	}

	@Override
	public String toString() {
		return "RedisWriterArgs [multiExec=" + multiExec + ", waitReplicas=" + waitReplicas + ", waitTimeout="
				+ waitTimeout + ", merge=" + merge + "]";
	}

}
