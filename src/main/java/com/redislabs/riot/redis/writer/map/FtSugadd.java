package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.writer.KeyBuilder;

import lombok.Builder;

public class FtSugadd extends AbstractFtSugadd {

	@Builder
	protected FtSugadd(KeyBuilder keyBuilder, boolean keepKeyFields, String field, String score, double defaultScore,
			boolean increment) {
		super(keyBuilder, keepKeyFields, field, score, defaultScore, increment);
	}

	@Override
	protected String payload(Map<String, Object> item) {
		return null;
	}

}