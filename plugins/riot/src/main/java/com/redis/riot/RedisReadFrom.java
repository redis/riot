package com.redis.riot;

import io.lettuce.core.ReadFrom;

public enum RedisReadFrom {

	MASTER(ReadFrom.MASTER), MASTER_PREFERRED(ReadFrom.MASTER_PREFERRED),

	UPSTREAM(ReadFrom.UPSTREAM), UPSTREAM_PREFERRED(ReadFrom.UPSTREAM_PREFERRED),

	REPLICA_PREFERRED(ReadFrom.REPLICA_PREFERRED), REPLICA(ReadFrom.REPLICA),

	LOWEST_LATENCY(ReadFrom.LOWEST_LATENCY),

	ANY(ReadFrom.ANY), ANY_REPLICA(ReadFrom.ANY_REPLICA);

	private final ReadFrom readFrom;

	private RedisReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	public ReadFrom getReadFrom() {
		return readFrom;
	}

}