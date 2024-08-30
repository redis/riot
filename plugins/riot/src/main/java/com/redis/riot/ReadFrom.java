package com.redis.riot;

public enum ReadFrom {

	UPSTREAM(io.lettuce.core.ReadFrom.UPSTREAM), UPSTREAM_PREFERRED(io.lettuce.core.ReadFrom.UPSTREAM_PREFERRED),

	REPLICA_PREFERRED(io.lettuce.core.ReadFrom.REPLICA_PREFERRED), REPLICA(io.lettuce.core.ReadFrom.REPLICA),

	LOWEST_LATENCY(io.lettuce.core.ReadFrom.LOWEST_LATENCY),

	ANY(io.lettuce.core.ReadFrom.ANY), ANY_REPLICA(io.lettuce.core.ReadFrom.ANY_REPLICA);

	private final io.lettuce.core.ReadFrom readFrom;

	private ReadFrom(io.lettuce.core.ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	public io.lettuce.core.ReadFrom getReadFrom() {
		return readFrom;
	}

}