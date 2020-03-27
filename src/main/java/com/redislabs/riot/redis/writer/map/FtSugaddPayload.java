package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.writer.KeyBuilder;

import lombok.Builder;
import lombok.Setter;

public class FtSugaddPayload extends AbstractFtSugadd {

	private @Setter String payload;

	@Builder
	protected FtSugaddPayload(KeyBuilder keyBuilder, boolean keepKeyFields, String field, String score,
			double defaultScore, boolean increment, String payload) {
		super(keyBuilder, keepKeyFields, field, score, defaultScore, increment);
		this.payload = payload;
	}

	@Override
	protected String payload(Map<String, Object> item) {
		return convert(item.remove(payload), String.class);
	}

}