package com.redis.riot.cli.common;

import io.lettuce.core.ReadFrom;

public enum ReadFromEnum {

	MASTER(ReadFrom.MASTER), MASTER_PREFERRED(ReadFrom.MASTER_PREFERRED),

	UPSTREAM(ReadFrom.UPSTREAM), UPSTREAM_PREFERRED(ReadFrom.UPSTREAM_PREFERRED),

	REPLICA_PREFERRED(ReadFrom.REPLICA_PREFERRED), REPLICA(ReadFrom.REPLICA),

	LOWEST_LATENCY(ReadFrom.LOWEST_LATENCY),

	ANY(ReadFrom.ANY), ANY_REPLICA(ReadFrom.ANY_REPLICA);

	private final ReadFrom value;

	private ReadFromEnum(ReadFrom value) {
		this.value = value;
	}

	public ReadFrom getValue() {
		return value;
	}

}
