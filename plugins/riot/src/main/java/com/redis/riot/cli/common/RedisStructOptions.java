package com.redis.riot.cli.common;

import com.redis.spring.batch.writer.MergePolicy;
import com.redis.spring.batch.writer.StreamIdPolicy;
import com.redis.spring.batch.writer.StructOptions;

import picocli.CommandLine.Option;

public class RedisStructOptions {

	@Option(names = "--merge-policy", description = "Policy to merge collection data structures: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<p>")
	private MergePolicy mergePolicy = MergePolicy.OVERWRITE;

	@Option(names = "--stream-id", description = "Policy for stream message IDs: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<p>")
	private StreamIdPolicy streamIdPolicy = StreamIdPolicy.PROPAGATE;

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

	public StructOptions structOptions() {
		return StructOptions.builder().mergePolicy(mergePolicy).streamIdPolicy(streamIdPolicy).build();
	}

	@Override
	public String toString() {
		return "RedisStructOptions [mergePolicy=" + mergePolicy + ", streamIdPolicy=" + streamIdPolicy + "]";
	}

}
