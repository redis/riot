package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.riot.redis.writer.KeyBuilder;

import lombok.Builder;
import lombok.Setter;

public class FtAddPayload extends AbstractFtAdd {

	private @Setter String payload;

	@Builder
	protected FtAddPayload(KeyBuilder keyBuilder, boolean keepKeyFields, String index, String score,
			double defaultScore, AddOptions options, String payload) {
		super(keyBuilder, keepKeyFields, index, score, defaultScore, options);
		this.payload = payload;
	}

	@Override
	protected String payload(Map<String, Object> item) {
		return convert(item.remove(payload), String.class);
	}

}
