package com.redis.riot;

import java.time.Duration;

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
	private int waitReplicas = 0;
	@Option(names = "--wait-timeout", description = "Timeout in millis for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long waitTimeout = 300;

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
		Builder options = WriterOptions.builder();
		if (waitReplicas > 0) {
			options.waitForReplication(WaitForReplication.of(waitReplicas, Duration.ofMillis(waitTimeout)));
		}
		options.multiExec(multiExec);
		return options.build();
	}

}
