package com.redis.riot;

import com.redis.spring.batch.RedisItemWriter.RedisItemWriterBuilder;

import lombok.Data;
import picocli.CommandLine.Option;

@Data
public class RedisWriterOptions {

	@Option(names = "--multi-exec", description = "Enable MULTI/EXEC writes.")
	private boolean multiExec;
	@Option(names = "--wait-replicas", description = "Number of replicas for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int waitReplicas = 0;
	@Option(names = "--wait-timeout", description = "Timeout in millis for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long waitTimeout = 300;
	@Option(names = "--writer-pool", description = "Max pool connections (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolMax = 8;

	public <B extends RedisItemWriterBuilder<String, String, ?>> B configure(B writer) {
		if (waitReplicas > 0) {
			writer.waitForReplication(waitReplicas, waitTimeout);
		}
		if (multiExec) {
			writer.multiExec();
		}
		return writer;
	}

}
