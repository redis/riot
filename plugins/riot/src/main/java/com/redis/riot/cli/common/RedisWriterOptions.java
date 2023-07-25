package com.redis.riot.cli.common;

import com.redis.spring.batch.writer.MergePolicy;
import com.redis.spring.batch.writer.StreamIdPolicy;
import com.redis.spring.batch.writer.TtlPolicy;
import com.redis.spring.batch.writer.WriterOptions;

import picocli.CommandLine.Option;

public class RedisWriterOptions {

	@Option(names = "--no-ttl", description = "Disables key expiry.")
	private boolean noTtl;

	@Option(names = "--merge", description = "Merge collection data structures.")
	private boolean merge;

	@Option(names = "--no-id", description = "Disables propagation of stream message IDs.")
	private boolean noStreamId;

	public WriterOptions writerOptions() {
		return WriterOptions.builder().ttlPolicy(ttlPolicy()).mergePolicy(mergePolicy())
				.streamIdPolicy(streamIdPolicy()).build();
	}

	private StreamIdPolicy streamIdPolicy() {
		return noStreamId ? StreamIdPolicy.DROP : StreamIdPolicy.PROPAGATE;
	}

	private MergePolicy mergePolicy() {
		return merge ? MergePolicy.MERGE : MergePolicy.OVERWRITE;
	}

	private TtlPolicy ttlPolicy() {
		return noTtl ? TtlPolicy.DROP : TtlPolicy.PROPAGATE;
	}

	public boolean isNoTtl() {
		return noTtl;
	}

	public void setNoTtl(boolean noTtl) {
		this.noTtl = noTtl;
	}

	public boolean isMerge() {
		return merge;
	}

	public void setMerge(boolean merge) {
		this.merge = merge;
	}

	public boolean isNoStreamId() {
		return noStreamId;
	}

	public void setNoStreamId(boolean noStreamId) {
		this.noStreamId = noStreamId;
	}

}
